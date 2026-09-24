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
// transient peak. A repetition that fails or does not demonstrate the rate
// shows that the selected rate was such a peak: it bounds the bracket from
// above like a failed probe and the search resumes below it, for at most
// MaxValidationRounds rounds.

const (
	DefaultMaximumRate = 1_000_000
	DefaultMaxProbes   = 24

	RequiredFullRepetitions = 5
	// MaxValidationRounds bounds how many selected rates the search may try
	// to validate. Each round that a repetition fails or does not demonstrate
	// moves the search below that rate; after this many rounds the search
	// ends inconclusive rather than descending further.
	MaxValidationRounds = 3

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
	// SearchValidationRoundsExhausted is a search whose selected rate failed
	// validation MaxValidationRounds times.
	SearchValidationRoundsExhausted SearchStatus = "validation-rounds-exhausted"
)

type ProbeRecord struct {
	Rate    int          `json:"rate"`
	Outcome ProbeOutcome `json:"outcome"`
}

// ValidationRound is the repetitions run at one selected rate, in execution
// order. A round ends at five passing repetitions or at its first repetition
// that did not pass.
type ValidationRound struct {
	Rate     int            `json:"rate"`
	Outcomes []ProbeOutcome `json:"outcomes"`
}

// allPassed reports whether every repetition of the round so far passed.
func (round ValidationRound) allPassed() bool {
	for _, outcome := range round.Outcomes {
		if outcome != ProbePassing {
			return false
		}
	}
	return true
}

// open reports whether the round still takes repetitions: every outcome so
// far passed and fewer than the required number ran.
func (round ValidationRound) open() bool {
	return len(round.Outcomes) < RequiredFullRepetitions && round.allPassed()
}

// validated reports whether the round's every required repetition passed.
func (round ValidationRound) validated() bool {
	return len(round.Outcomes) == RequiredFullRepetitions && round.allPassed()
}

// CapacitySearch is a strict-replay bounded search state machine. Probes and
// validation repetitions must be recorded at exactly the rate the search
// selected; any deviation is an error so an executed campaign cannot silently
// reorder its evidence.
type CapacitySearch struct {
	maximum   int
	probes    []ProbeRecord
	rounds    []ValidationRound
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

// ValidationRounds returns the recorded validation rounds in execution order.
func (search *CapacitySearch) ValidationRounds() []ValidationRound {
	rounds := make([]ValidationRound, len(search.rounds))
	for index, round := range search.rounds {
		rounds[index] = ValidationRound{Rate: round.Rate, Outcomes: append([]ProbeOutcome(nil), round.Outcomes...)}
	}
	return rounds
}

// NextRepetitionRate returns the rate the next validation repetition must run
// at: the bracketed lower passing rate, while its round is not yet decided.
// The second result is false when no repetition is pending.
func (search *CapacitySearch) NextRepetitionRate() (int, bool) {
	if search.status != SearchBracketed {
		return 0, false
	}
	if count := len(search.rounds); count != 0 {
		last := search.rounds[count-1]
		if last.Rate == search.lower && !last.open() {
			return 0, false
		}
	}
	return search.lower, true
}

// RecordRepetition adds one validation repetition outcome. The rate must equal
// the selected NextRepetitionRate. Five passing repetitions validate the rate.
// A failed or not-demonstrated repetition shows that the rate was a transient
// peak rather than a sustained rate: it bounds the bracket from above like a
// failed probe, the highest passing probe below it becomes the lower bound,
// and the search resumes, at most MaxValidationRounds rounds in all. An
// inconclusive repetition (missing or invalid evidence) says nothing about the
// rate and ends validation. No repetition is ever retried.
func (search *CapacitySearch) RecordRepetition(rate int, outcome ProbeOutcome) error {
	next, pending := search.NextRepetitionRate()
	if !pending {
		return errors.New("no validation repetition is pending")
	}
	if rate != next {
		return fmt.Errorf("repetition rate %d does not match the selected rate %d", rate, next)
	}
	switch outcome {
	case ProbePassing, ProbeFailing, ProbeNotDemonstrated, ProbeInconclusive:
	default:
		return fmt.Errorf("repetition returned an unknown outcome %q", outcome)
	}
	if count := len(search.rounds); count == 0 || search.rounds[count-1].Rate != rate || !search.rounds[count-1].open() {
		search.rounds = append(search.rounds, ValidationRound{Rate: rate})
	}
	round := &search.rounds[len(search.rounds)-1]
	round.Outcomes = append(round.Outcomes, outcome)
	if outcome == ProbeFailing || outcome == ProbeNotDemonstrated {
		search.rejectLower()
	}
	return nil
}

// rejectLower moves the bracket below a selected rate that failed validation
// and resumes the search, or ends it once MaxValidationRounds rounds failed.
func (search *CapacitySearch) rejectLower() {
	search.upper = search.lower
	search.lower = 0
	for _, probe := range search.probes {
		if probe.Outcome == ProbePassing && probe.Rate < search.upper && probe.Rate > search.lower {
			search.lower = probe.Rate
		}
	}
	if len(search.rounds) >= MaxValidationRounds {
		search.status = SearchValidationRoundsExhausted
		return
	}
	search.status = SearchRunning
	search.advance()
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
	SearchIncompleteReason          = "search-incomplete"
	SearchNotRefinedReason          = "search-did-not-refine-a-pass-fail-bracket"
	NoPassingRateReason             = "no-passing-rate"
	NoDemonstratedRateReason        = "no-demonstrated-rate-with-undemonstrated-probes"
	RepetitionsMissingReason        = "validation-repetitions-missing"
	RepetitionInconclusiveReason    = "validation-repetition-inconclusive"
	ValidationRoundsExhaustedReason = "validation-rounds-exhausted"
)

// DecideCapacity maps a search and its validation rounds to a campaign
// decision. Only a bracket refined to within five percent proceeds to
// validation; five full repetitions at the selected lower passing rate must
// all pass, and that lower rate is the result. Lower-bound-only,
// integer-resolution-limit, budget exhaustion and validation-round exhaustion
// are inconclusive, never a widened pass.
func DecideCapacity(search *CapacitySearch) CapacityDecision {
	status := search.Status()
	switch status {
	case SearchRunning:
		return CapacityDecision{Decision: Inconclusive, Status: status, Reason: SearchIncompleteReason}
	case SearchNoPassingRate:
		// Failure needs demonstrated failures. A search that found no
		// demonstrated rate only because some probe or repetition was not
		// demonstrated has not shown that the workload cannot be sustained.
		for _, probe := range search.probes {
			if probe.Outcome == ProbeNotDemonstrated {
				return CapacityDecision{Decision: Inconclusive, Status: status, Reason: NoDemonstratedRateReason}
			}
		}
		for _, round := range search.rounds {
			for _, outcome := range round.Outcomes {
				if outcome == ProbeNotDemonstrated {
					return CapacityDecision{Decision: Inconclusive, Status: status, Reason: NoDemonstratedRateReason}
				}
			}
		}
		return CapacityDecision{Decision: Fail, Status: status, Reason: NoPassingRateReason}
	case SearchValidationRoundsExhausted:
		return CapacityDecision{Decision: Inconclusive, Status: status, Reason: ValidationRoundsExhaustedReason}
	case SearchBracketed:
	default:
		return CapacityDecision{Decision: Inconclusive, Status: status, Reason: SearchNotRefinedReason}
	}

	selected := search.Lower()
	count := len(search.rounds)
	if count == 0 || search.rounds[count-1].Rate != selected {
		return CapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: RepetitionsMissingReason}
	}
	round := search.rounds[count-1]
	switch {
	case round.validated():
		return CapacityDecision{Decision: Pass, Status: status, SelectedRate: selected}
	case round.open():
		return CapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: RepetitionsMissingReason}
	default:
		// A closed round at the selected rate that did not validate ended
		// at an inconclusive repetition: a failed one would have moved the
		// search below this rate.
		return CapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: RepetitionInconclusiveReason}
	}
}
