// Frozen capacity search from commit 9c86a37, internal/perfstats/capacity.go.
// Only top-level names are prefixed to coexist with the current search.
// Keep this oracle independent of production search logic.

package perfstats

import (
	"errors"
	"fmt"
	"math"
)

// This file implements the predeclared bounded capacity search: integer
// rates, a bracket between the highest demonstrated rate and the lowest rate
// that failed or was not demonstrated, refined to within five percent,
// bounded repeats for backlog-only undecided windows, and a fixed decided-probe
// budget. It deliberately does not widen those semantics: a
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
// BaselineMaxValidationRounds rounds.

const (
	BaselineDefaultMaximumRate = 1_000_000
	BaselineDefaultMaxProbes   = 24

	BaselineRequiredFullRepetitions = 5
	// BaselineMaxRateAttempts bounds attempts for one probe or validation repetition.
	// Only backlog-growth bounds straddling the floor qualify for another
	// attempt. The owner decision amends the no-retry rules in #105 and #140:
	// https://github.com/gomaja/go-m3ua/issues/44#issuecomment-6100413906
	BaselineMaxRateAttempts = 3
	// BaselineMaxValidationRounds bounds how many selected rates the search may try
	// to validate. Each round that a repetition fails or does not demonstrate
	// moves the search below that rate; after this many rounds the search
	// ends inconclusive rather than descending further.
	BaselineMaxValidationRounds = 3

	// BaselineMaximumSearchRate is the largest rate the search accepts. Every probe
	// rate the search can select is bounded by the configured maximum, and
	// the widest products advance() forms from those rates are 105*lower and
	// 100*upper, so bounding the maximum at math.MaxInt/105 keeps the bracket
	// comparison, the doubling step and the midpoint exact. Without it a
	// large but "valid" maximum wraps 2*lower to a negative probe rate and
	// wraps 100*upper negative, which satisfies the five-percent comparison
	// on an arbitrarily wide bracket and lets a capacity campaign pass.
	BaselineMaximumSearchRate = math.MaxInt / 105
)

type BaselineProbeOutcome string

const (
	BaselineProbePassing BaselineProbeOutcome = "pass"
	BaselineProbeFailing BaselineProbeOutcome = "fail"
	// BaselineProbeBacklogUndecided requests the same rate again without contributing
	// a probe or repetition outcome. After BaselineMaxRateAttempts such windows, the
	// rate is recorded once as BaselineProbeNotDemonstrated. Callers must use this only
	// when backlog straddling is the run's sole inconclusive cause.
	BaselineProbeBacklogUndecided BaselineProbeOutcome = "backlog-undecided"
	// BaselineProbeNotDemonstrated is a probe that did not demonstrate a sustained
	// rate for a rate-related reason: a transport stall, backlog growth bounds
	// still straddling the floor after BaselineMaxRateAttempts, or a straddle with
	// another non-passing verdict that rules out repeats. Backlog-only
	// straddles must first be recorded as BaselineProbeBacklogUndecided. The rate bounds
	// the bracket from above exactly as a failure does, and the search continues.
	// Only demonstrated rates pass.
	BaselineProbeNotDemonstrated BaselineProbeOutcome = "not-demonstrated"
	// BaselineProbeInconclusive is a probe whose evidence was missing or invalid. It
	// says nothing about the rate, so it terminates the search.
	BaselineProbeInconclusive BaselineProbeOutcome = "inconclusive"
)

type BaselineSearchStatus string

const (
	BaselineSearchRunning                BaselineSearchStatus = ""
	BaselineSearchBracketed              BaselineSearchStatus = "bracketed"
	BaselineSearchIntegerResolutionLimit BaselineSearchStatus = "integer-resolution-limit"
	BaselineSearchLowerBoundOnly         BaselineSearchStatus = "lower-bound-only"
	BaselineSearchNoPassingRate          BaselineSearchStatus = "no-passing-rate"
	BaselineSearchInconclusive           BaselineSearchStatus = "inconclusive"
	BaselineSearchProbeBudgetExhausted   BaselineSearchStatus = "probe-budget-exhausted"
	// BaselineSearchValidationRoundsExhausted is a search whose selected rate failed
	// validation BaselineMaxValidationRounds times.
	BaselineSearchValidationRoundsExhausted BaselineSearchStatus = "validation-rounds-exhausted"
)

type BaselineProbeRecord struct {
	Rate    int                  `json:"rate"`
	Outcome BaselineProbeOutcome `json:"outcome"`
}

// BaselineValidationRound is the repetitions run at one selected rate, in execution
// order. A round ends at five passing repetitions or at its first repetition
// that did not pass.
type BaselineValidationRound struct {
	Rate     int                    `json:"rate"`
	Outcomes []BaselineProbeOutcome `json:"outcomes"`
}

// allPassed reports whether every repetition of the round so far passed.
func (round BaselineValidationRound) allPassed() bool {
	for _, outcome := range round.Outcomes {
		if outcome != BaselineProbePassing {
			return false
		}
	}
	return true
}

// open reports whether the round still takes repetitions: every outcome so
// far passed and fewer than the required number ran.
func (round BaselineValidationRound) open() bool {
	return len(round.Outcomes) < BaselineRequiredFullRepetitions && round.allPassed()
}

// validated reports whether the round's every required repetition passed.
func (round BaselineValidationRound) validated() bool {
	return len(round.Outcomes) == BaselineRequiredFullRepetitions && round.allPassed()
}

// BaselineCapacitySearch is a strict-replay bounded search state machine. Probes and
// validation repetitions must be recorded at exactly the rate the search
// selected; any deviation is an error so an executed campaign cannot silently
// reorder its evidence.
type BaselineCapacitySearch struct {
	maximum   int
	probes    []BaselineProbeRecord
	rounds    []BaselineValidationRound
	lower     int
	upper     int
	next      int
	status    BaselineSearchStatus
	maxProbes int
	// undecidedAttempts belongs to the current probe or validation repetition,
	// not to all repetitions at a selected rate. Deciding it resets the count.
	undecidedAttempts int
}

// BaselineNewCapacitySearch validates the search bounds: positive integer initial,
// maximum and probe budget, initial not above maximum.
func BaselineNewCapacitySearch(initial, maximum, maxProbes int) (*BaselineCapacitySearch, error) {
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
	if maximum > BaselineMaximumSearchRate {
		return nil, fmt.Errorf("maximum must not exceed %d", BaselineMaximumSearchRate)
	}
	return &BaselineCapacitySearch{maximum: maximum, maxProbes: maxProbes, next: initial}, nil
}

// Status is the terminal search status, or BaselineSearchRunning while more probes
// are required.
func (search *BaselineCapacitySearch) Status() BaselineSearchStatus {
	return search.status
}

// Lower is the highest proven passing rate, or zero when none exists.
func (search *BaselineCapacitySearch) Lower() int {
	return search.lower
}

// Upper is the lowest proven failing rate, or zero when none exists.
func (search *BaselineCapacitySearch) Upper() int {
	return search.upper
}

// Probes returns the decided probe history in execution order. Deferred
// backlog-only attempts do not appear here or consume the probe budget.
func (search *BaselineCapacitySearch) Probes() []BaselineProbeRecord {
	return append([]BaselineProbeRecord(nil), search.probes...)
}

// ValidationRounds returns the recorded validation rounds in execution order.
func (search *BaselineCapacitySearch) ValidationRounds() []BaselineValidationRound {
	rounds := make([]BaselineValidationRound, len(search.rounds))
	for index, round := range search.rounds {
		rounds[index] = BaselineValidationRound{Rate: round.Rate, Outcomes: append([]BaselineProbeOutcome(nil), round.Outcomes...)}
	}
	return rounds
}

// NextRepetitionRate returns the rate the next validation repetition must run
// at: the bracketed lower passing rate, while its round is not yet decided.
// The second result is false when no repetition is pending.
func (search *BaselineCapacitySearch) NextRepetitionRate() (int, bool) {
	if search.status != BaselineSearchBracketed {
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
// and the search resumes, at most BaselineMaxValidationRounds rounds in all. An
// inconclusive repetition (missing or invalid evidence) says nothing about the
// rate and ends validation. A backlog-only undecided attempt leaves the same
// repetition pending, for at most BaselineMaxRateAttempts runs in all.
func (search *BaselineCapacitySearch) RecordRepetition(rate int, outcome BaselineProbeOutcome) error {
	next, pending := search.NextRepetitionRate()
	if !pending {
		return errors.New("no validation repetition is pending")
	}
	if rate != next {
		return fmt.Errorf("repetition rate %d does not match the selected rate %d", rate, next)
	}
	switch outcome {
	case BaselineProbePassing, BaselineProbeFailing, BaselineProbeNotDemonstrated, BaselineProbeInconclusive, BaselineProbeBacklogUndecided:
	default:
		return fmt.Errorf("repetition returned an unknown outcome %q", outcome)
	}
	outcome = search.resolveAttempt(outcome)
	if outcome == BaselineProbeBacklogUndecided {
		return nil
	}
	if count := len(search.rounds); count == 0 || search.rounds[count-1].Rate != rate || !search.rounds[count-1].open() {
		search.rounds = append(search.rounds, BaselineValidationRound{Rate: rate})
	}
	round := &search.rounds[len(search.rounds)-1]
	round.Outcomes = append(round.Outcomes, outcome)
	if outcome == BaselineProbeFailing || outcome == BaselineProbeNotDemonstrated {
		search.rejectLower()
	}
	return nil
}

// rejectLower moves the bracket below a selected rate that failed validation
// and resumes the search, or ends it once BaselineMaxValidationRounds rounds failed.
func (search *BaselineCapacitySearch) rejectLower() {
	search.upper = search.lower
	search.lower = 0
	for _, probe := range search.probes {
		if probe.Outcome == BaselineProbePassing && probe.Rate < search.upper && probe.Rate > search.lower {
			search.lower = probe.Rate
		}
	}
	if len(search.rounds) >= BaselineMaxValidationRounds {
		search.status = BaselineSearchValidationRoundsExhausted
		return
	}
	search.status = BaselineSearchRunning
	search.advance()
}

// NextRate returns the rate the search selected for the next probe. The
// second result is false once the search has terminated.
func (search *BaselineCapacitySearch) NextRate() (int, bool) {
	if search.status != BaselineSearchRunning {
		return 0, false
	}
	return search.next, true
}

// Record adds one probe outcome. The rate must equal the selected NextRate;
// an unknown outcome or a finished search is an error. A not-demonstrated
// probe bounds the bracket from above like a failure; an inconclusive probe
// terminates the search. A backlog-only undecided attempt leaves the same
// probe pending without using its budget, up to BaselineMaxRateAttempts runs in all.
func (search *BaselineCapacitySearch) Record(rate int, outcome BaselineProbeOutcome) error {
	if search.status != BaselineSearchRunning {
		if next, pending := search.NextRepetitionRate(); pending {
			return fmt.Errorf("no probe is pending: the search awaits a validation repetition at %d", next)
		}
		return fmt.Errorf("search already terminated with status %q", search.status)
	}
	if rate != search.next {
		return fmt.Errorf("probe rate %d does not match the selected rate %d", rate, search.next)
	}
	switch outcome {
	case BaselineProbePassing, BaselineProbeFailing, BaselineProbeNotDemonstrated, BaselineProbeInconclusive, BaselineProbeBacklogUndecided:
	default:
		return fmt.Errorf("probe returned an unknown outcome %q", outcome)
	}
	outcome = search.resolveAttempt(outcome)
	if outcome == BaselineProbeBacklogUndecided {
		return nil
	}
	search.probes = append(search.probes, BaselineProbeRecord{Rate: rate, Outcome: outcome})
	if outcome == BaselineProbeInconclusive {
		search.status = BaselineSearchInconclusive
		return nil
	}
	if outcome == BaselineProbePassing {
		search.lower = rate
	} else {
		search.upper = rate
	}
	search.advance()
	return nil
}

// NextAttempt is the one-based attempt number for the pending probe or
// validation repetition, or zero when no run is pending. A new repetition at
// the same selected rate starts at one again.
func (search *BaselineCapacitySearch) NextAttempt() int {
	_, probe := search.NextRate()
	_, repetition := search.NextRepetitionRate()
	if !probe && !repetition {
		return 0
	}
	return search.undecidedAttempts + 1
}

// resolveAttempt defers only backlog-only undecided windows. It runs after
// rate and outcome validation, so rejected input cannot spend an attempt.
func (search *BaselineCapacitySearch) resolveAttempt(outcome BaselineProbeOutcome) BaselineProbeOutcome {
	if outcome == BaselineProbeBacklogUndecided {
		search.undecidedAttempts++
		if search.undecidedAttempts < BaselineMaxRateAttempts {
			return outcome
		}
		outcome = BaselineProbeNotDemonstrated
	}
	search.undecidedAttempts = 0
	return outcome
}

func (search *BaselineCapacitySearch) advance() {
	if search.lower != 0 && search.upper != 0 {
		switch {
		case 100*search.upper <= 105*search.lower:
			search.status = BaselineSearchBracketed
			return
		case search.upper-search.lower == 1:
			search.status = BaselineSearchIntegerResolutionLimit
			return
		default:
			search.next = (search.lower + search.upper) / 2
		}
	} else if search.lower != 0 {
		if search.lower == search.maximum {
			search.status = BaselineSearchLowerBoundOnly
			return
		}
		search.next = min(2*search.lower, search.maximum)
	} else {
		if search.upper == 1 {
			search.status = BaselineSearchNoPassingRate
			return
		}
		search.next = max(1, search.upper/2)
	}
	if len(search.probes) >= search.maxProbes {
		search.status = BaselineSearchProbeBudgetExhausted
	}
}

// BaselineCapacityDecision is the campaign-level outcome of one completed capacity
// search plus its validation repetitions.
type BaselineCapacityDecision struct {
	Decision     Decision             `json:"decision"`
	Status       BaselineSearchStatus `json:"search_status"`
	SelectedRate int                  `json:"selected_rate,omitempty"`
	Reason       string               `json:"reason,omitempty"`
}

const (
	BaselineSearchIncompleteReason          = "search-incomplete"
	BaselineSearchNotRefinedReason          = "search-did-not-refine-a-pass-fail-bracket"
	BaselineNoPassingRateReason             = "no-passing-rate"
	BaselineNoDemonstratedRateReason        = "no-demonstrated-rate-with-undemonstrated-probes"
	BaselineRejectedSearchNotRefinedReason  = "search-did-not-refine-a-bracket-below-a-rejected-rate"
	BaselineRepetitionsMissingReason        = "validation-repetitions-missing"
	BaselineRepetitionInconclusiveReason    = "validation-repetition-inconclusive"
	BaselineValidationRoundsExhaustedReason = "validation-rounds-exhausted"
)

// BaselineDecideCapacity maps a search and its validation rounds to a campaign
// decision. Only a bracket refined to within five percent proceeds to
// validation; five full repetitions at the selected lower passing rate must
// all pass, and that lower rate is the result. Lower-bound-only,
// integer-resolution-limit, budget exhaustion and validation-round exhaustion
// are inconclusive, never a widened pass.
func BaselineDecideCapacity(search *BaselineCapacitySearch) BaselineCapacityDecision {
	status := search.Status()
	switch status {
	case BaselineSearchRunning:
		return BaselineCapacityDecision{Decision: Inconclusive, Status: status, Reason: BaselineSearchIncompleteReason}
	case BaselineSearchNoPassingRate:
		// Failure needs demonstrated failures. A search that found no
		// demonstrated rate only because some probe or repetition was not
		// demonstrated has not shown that the workload cannot be sustained.
		for _, probe := range search.probes {
			if probe.Outcome == BaselineProbeNotDemonstrated {
				return BaselineCapacityDecision{Decision: Inconclusive, Status: status, Reason: BaselineNoDemonstratedRateReason}
			}
		}
		for _, round := range search.rounds {
			for _, outcome := range round.Outcomes {
				if outcome == BaselineProbeNotDemonstrated {
					return BaselineCapacityDecision{Decision: Inconclusive, Status: status, Reason: BaselineNoDemonstratedRateReason}
				}
			}
		}
		return BaselineCapacityDecision{Decision: Fail, Status: status, Reason: BaselineNoPassingRateReason}
	case BaselineSearchValidationRoundsExhausted:
		return BaselineCapacityDecision{Decision: Inconclusive, Status: status, Reason: BaselineValidationRoundsExhaustedReason}
	case BaselineSearchBracketed:
	default:
		if len(search.rounds) != 0 {
			// It refined a bracket once; validation rejected that rate, and
			// the search below it then ended without a new one.
			return BaselineCapacityDecision{Decision: Inconclusive, Status: status, Reason: BaselineRejectedSearchNotRefinedReason}
		}
		return BaselineCapacityDecision{Decision: Inconclusive, Status: status, Reason: BaselineSearchNotRefinedReason}
	}

	selected := search.Lower()
	count := len(search.rounds)
	if count == 0 || search.rounds[count-1].Rate != selected {
		return BaselineCapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: BaselineRepetitionsMissingReason}
	}
	round := search.rounds[count-1]
	switch {
	case round.validated():
		return BaselineCapacityDecision{Decision: Pass, Status: status, SelectedRate: selected}
	case round.open():
		return BaselineCapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: BaselineRepetitionsMissingReason}
	default:
		// A closed round at the selected rate that did not validate ended
		// at an inconclusive repetition: a failed one would have moved the
		// search below this rate.
		return BaselineCapacityDecision{Decision: Inconclusive, Status: status, SelectedRate: selected, Reason: BaselineRepetitionInconclusiveReason}
	}
}
