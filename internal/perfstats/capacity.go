package perfstats

import (
	"errors"
	"fmt"
	"math"
)

// This file implements the predeclared bounded capacity search: integer
// rates, a bracket between the highest demonstrated rate and the lowest rate
// that failed or was not demonstrated, refined to within five percent, no
// probe retries, and a fixed probe budget. It deliberately does not widen those semantics: a
// search that cannot refine the bracket, finds no upper failure bound, or
// exhausts its budget is not a capacity measurement.
//
// A probe rate is passing only when the full run at that rate satisfies the
// predeclared sustained-backlog decision method (see backlog.go). After the
// bracket is refined, five full repetitions at the selected lower passing
// rate must all pass; the lower passing rate is the result, never a
// transient peak.

const (
	DefaultMaximumRate = 1_000_000
	DefaultMaxProbes   = 24

	RequiredFullRepetitions = 5

	// MaximumSearchRate is the largest rate the search accepts. Every probe
	// rate the search can select is bounded by the configured maximum, and
	// the widest products advance() forms from those rates are 105*lower and
	// 100*upper, so bounding the maximum at math.MaxInt/105 keeps the bracket
	// comparison, the doubling step and the midpoint exact. Without it a
	// large but "valid" maximum wraps 2*lower to a negative probe rate and
	// wraps 100*upper negative, which satisfies the five-percent comparison
	// on an arbitrarily wide bracket and lets a capacity campaign pass.
	MaximumSearchRate = math.MaxInt / 105
)

type ProbeOutcome string

const (
	ProbePassing ProbeOutcome = "pass"
	ProbeFailing ProbeOutcome = "fail"
	// ProbeNotDemonstrated is a probe that did not demonstrate a sustained
	// rate for a rate-related reason: a transport stall, or backlog growth
	// bounds that straddle the floor. Near and above capacity those are the
	// expected outcomes, so the rate bounds the bracket from above exactly as a
	// failure does, and the search continues. Only demonstrated rates pass.
	ProbeNotDemonstrated ProbeOutcome = "not-demonstrated"
	// ProbeInconclusive is a probe whose evidence was missing or invalid. It
	// says nothing about the rate, so it terminates the search.
	ProbeInconclusive ProbeOutcome = "inconclusive"
)

type SearchStatus string

const (
	SearchRunning                SearchStatus = ""
	SearchBracketed              SearchStatus = "bracketed"
	SearchIntegerResolutionLimit SearchStatus = "integer-resolution-limit"
	SearchLowerBoundOnly         SearchStatus = "lower-bound-only"
	SearchNoPassingRate          SearchStatus = "no-passing-rate"
	SearchInconclusive           SearchStatus = "inconclusive"
	SearchProbeBudgetExhausted   SearchStatus = "probe-budget-exhausted"
)

type ProbeRecord struct {
	Rate    int          `json:"rate"`
	Outcome ProbeOutcome `json:"outcome"`
}

// CapacitySearch is a strict-replay bounded search state machine. Probes must
// be recorded at exactly the rate the search selected; any deviation is an
// error so an executed campaign cannot silently reorder its evidence.
type CapacitySearch struct {
	maximum   int
	probes    []ProbeRecord
	lower     int
	upper     int
	next      int
	status    SearchStatus
	maxProbes int
}

// NewCapacitySearch validates the search bounds: positive integer initial,
// maximum and probe budget, initial not above maximum.
func NewCapacitySearch(initial, maximum, maxProbes int) (*CapacitySearch, error) {
	for name, value := range map[string]int{"initial": initial, "maximum": maximum, "max_probes": maxProbes} {
		if value <= 0 {
			return nil, fmt.Errorf("%s must be a positive integer", name)
		}
	}
	if initial > maximum {
		return nil, errors.New("initial exceeds maximum")
	}
	// initial is already bounded by maximum, and every rate the search can
	// select afterwards is bounded by maximum too, so bounding maximum alone
	// keeps all of the search arithmetic exact.
	if maximum > MaximumSearchRate {
		return nil, fmt.Errorf("maximum must not exceed %d", MaximumSearchRate)
	}
	return &CapacitySearch{maximum: maximum, maxProbes: maxProbes, next: initial}, nil
}

// Status is the terminal search status, or SearchRunning while more probes
// are required.
func (search *CapacitySearch) Status() SearchStatus {
	return search.status
}

// Lower is the highest proven passing rate, or zero when none exists.
func (search *CapacitySearch) Lower() int {
	return search.lower
}

// Upper is the lowest proven failing rate, or zero when none exists.
func (search *CapacitySearch) Upper() int {
	return search.upper
}

// Probes returns the recorded probe history in execution order.
func (search *CapacitySearch) Probes() []ProbeRecord {
	return append([]ProbeRecord(nil), search.probes...)
}

// NextRate returns the rate the search selected for the next probe. The
// second result is false once the search has terminated.
func (search *CapacitySearch) NextRate() (int, bool) {
	if search.status != SearchRunning {
		return 0, false
	}
	return search.next, true
}

// Record adds one probe outcome. The rate must equal the selected NextRate;
// an unknown outcome or a finished search is an error. A not-demonstrated
// probe bounds the bracket from above like a failure; an inconclusive probe
// terminates the search. No probe is ever retried.
func (search *CapacitySearch) Record(rate int, outcome ProbeOutcome) error {
	if search.status != SearchRunning {
		return fmt.Errorf("search already terminated with status %q", search.status)
	}
	if rate != search.next {
		return fmt.Errorf("probe rate %d does not match the selected rate %d", rate, search.next)
	}
	switch outcome {
	case ProbePassing, ProbeFailing, ProbeNotDemonstrated, ProbeInconclusive:
	default:
		return fmt.Errorf("probe returned an unknown outcome %q", outcome)
	}
	search.probes = append(search.probes, ProbeRecord{Rate: rate, Outcome: outcome})
	if outcome == ProbeInconclusive {
		search.status = SearchInconclusive
		return nil
	}
	if outcome == ProbePassing {
		search.lower = rate
	} else {
		search.upper = rate
	}
	search.advance()
	return nil
}

func (search *CapacitySearch) advance() {
	if search.lower != 0 && search.upper != 0 {
		switch {
		case 100*search.upper <= 105*search.lower:
			search.status = SearchBracketed
			return
		case search.upper-search.lower == 1:
			search.status = SearchIntegerResolutionLimit
			return
		default:
			search.next = (search.lower + search.upper) / 2
		}
	} else if search.lower != 0 {
		if search.lower == search.maximum {
			search.status = SearchLowerBoundOnly
			return
		}
		search.next = min(2*search.lower, search.maximum)
	} else {
		if search.upper == 1 {
			search.status = SearchNoPassingRate
			return
		}
		search.next = max(1, search.upper/2)
	}
	if len(search.probes) >= search.maxProbes {
		search.status = SearchProbeBudgetExhausted
	}
}

// CapacityDecision is the campaign-level outcome of one completed capacity
// search plus its validation repetitions.
type CapacityDecision struct {
	Decision     Decision     `json:"decision"`
	Status       SearchStatus `json:"search_status"`
	SelectedRate int          `json:"selected_rate,omitempty"`
	Reason       string       `json:"reason,omitempty"`
}

const (
	SearchIncompleteReason       = "search-incomplete"
	SearchNotRefinedReason       = "search-did-not-refine-a-pass-fail-bracket"
	NoPassingRateReason          = "no-passing-rate"
	NoDemonstratedRateReason     = "no-demonstrated-rate-with-undemonstrated-probes"
	RepetitionsMissingReason     = "validation-repetitions-missing"
	RepetitionCountReason        = "validation-repetition-count-mismatch"
	RepetitionRateReason         = "validation-repetition-rate-mismatch"
	RepetitionFailureReason      = "validation-repetition-failed"
	RepetitionInconclusiveReason = "validation-repetition-inconclusive"
)

// DecideCapacity maps a terminated search and its validation repetitions to a
// campaign decision. Only a bracket refined to within five percent proceeds
// to repetitions; exactly five full repetitions at the selected lower passing
// rate must all pass, and that lower rate is the result. Lower-bound-only,
// integer-resolution-limit and budget exhaustion are inconclusive, never a
// widened pass.
func DecideCapacity(search *CapacitySearch, repetitionRates []int, repetitionDecisions []Decision) CapacityDecision {
	status := search.Status()
	switch status {
	case SearchRunning:
		return CapacityDecision{Decision: Inconclusive, Status: status, Reason: SearchIncompleteReason}
	case SearchNoPassingRate:
		// Failure needs demonstrated failures. A search that found no
		// demonstrated rate only because some probes were not demonstrated
		// has not shown that the workload cannot be sustained.
		for _, probe := range search.probes {
			if probe.Outcome == ProbeNotDemonstrated {
				return CapacityDecision{Decision: Inconclusive, Status: status, Reason: NoDemonstratedRateReason}
			}
		}
		return CapacityDecision{Decision: Fail, Status: status, Reason: NoPassingRateReason}
	case SearchBracketed:
	default:
		return CapacityDecision{Decision: Inconclusive, Status: status, Reason: SearchNotRefinedReason}
	}

	selected := search.Lower()
	if len(repetitionRates) == 0 && len(repetitionDecisions) == 0 {
		return CapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: RepetitionsMissingReason}
	}
	if len(repetitionRates) != RequiredFullRepetitions || len(repetitionDecisions) != RequiredFullRepetitions {
		return CapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: RepetitionCountReason}
	}
	for index, rate := range repetitionRates {
		if rate != selected {
			return CapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected,
				Reason: fmt.Sprintf("%s: repetition %d ran at %d, want %d", RepetitionRateReason, index+1, rate, selected)}
		}
	}
	for _, decision := range repetitionDecisions {
		switch decision {
		case Pass:
		case Fail:
			return CapacityDecision{Decision: Fail, Status: status, SelectedRate: selected, Reason: RepetitionFailureReason}
		default:
			return CapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: RepetitionInconclusiveReason}
		}
	}
	return CapacityDecision{Decision: Pass, Status: status, SelectedRate: selected}
}
