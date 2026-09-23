package main

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
)

// The DATA overload row of performance budgets section 4 offers twice the
// matched required rate for 60 s and then half of it for 60 s. The profile
// states that shape as rate multipliers of -rate and phase durations, for
// example "2x:60s,0.5x:60s".
const (
	// overloadRequestDeadline is the section 2 per-request deadline: a message
	// not accepted by the transport within two seconds of its scheduled
	// instant is refused, never sent late.
	overloadRequestDeadline = 2 * time.Second
	// overloadDrainMargin keeps every request deadline at least this far
	// before the absolute drain deadline, so sender workers always finish
	// before the drain watchdog and a refused request is never mistaken for a
	// stalled worker.
	overloadDrainMargin = time.Second
	// overloadRecoveryAllowance is the section 4 recovery bound: admitted
	// healthy throughput recovers within two seconds of the switch.
	overloadRecoveryAllowance = 2 * time.Second
	// overloadRecoveryTolerance is the fraction of the offered rate a
	// recovery window may fall short by, in addition to the backlog trend
	// rule's floor of 10 ms of offered traffic.
	overloadRecoveryTolerance = 0.01
	// overloadMinimumWindow excludes the short interval that ends at the
	// final pre-boundary probe: a recovery window is a one-second interval
	// between consecutive per-second progress observations.
	overloadMinimumWindow = 900 * time.Millisecond
	// overloadMinimumRecoveryPhase leaves the two-second allowance, a
	// one-second window and at least eight whole-second observations for the
	// backlog trend after it.
	overloadMinimumRecoveryPhase = 12 * time.Second
	overloadMaximumPhases        = 8
	overloadMaximumMultiplier    = 16
	// overloadMaximumMessages bounds each per-message outcome bitmap to 8 MiB.
	overloadMaximumMessages = 1 << 26
)

const (
	overloadRoleWarmup      = "warmup"
	overloadRoleMeasurement = "measurement"
)

// overloadSSNMRefusal is why an overload trial never runs with SSNM load. The
// DATA overload row offers DATA alone and judges its deliberate refusals and
// losses by its own contract; the SSNM rows judge SSNM delivery and budgets
// against loss-free DATA at a fixed nominal load. A combined run belongs to
// neither contract: each workload's verdict would rest on conditions the
// other deliberately breaks.
const overloadSSNMRefusal = "overload-profile cannot be combined with SSNM load (-ssnm-rate): the DATA overload row runs DATA alone, and the SSNM rows judge SSNM delivery against loss-free DATA"

// overloadPhase is one phase of an overload profile: the multiplier exactly
// as written, the offered rate it yields, and how long it lasts.
type overloadPhase struct {
	Multiplier string        `json:"multiplier"`
	Rate       uint64        `json:"rate"`
	Duration   time.Duration `json:"duration_ns"`
}

// overloadSpec identifies a cohort as part of an overload trial. The
// measurement cohort carries the phases it is scheduled by; the warm-up cohort
// is an ordinary loss-free cohort at the recovery rate and carries the
// identity only, so neither can be mistaken for a nominal capacity probe.
type overloadSpec struct {
	Profile         string          `json:"profile"`
	Role            string          `json:"role"`
	NominalRate     uint64          `json:"nominal_rate"`
	Phases          []overloadPhase `json:"phases,omitempty"`
	RequestDeadline time.Duration   `json:"request_deadline_ns,omitempty"`
}

func (spec *overloadSpec) measurement() bool {
	return spec != nil && spec.Role == overloadRoleMeasurement
}

func sameOverloadSpec(first, second *overloadSpec) bool {
	if first == nil || second == nil {
		return first == second
	}
	if first.Profile != second.Profile || first.Role != second.Role || first.NominalRate != second.NominalRate ||
		first.RequestDeadline != second.RequestDeadline || len(first.Phases) != len(second.Phases) {
		return false
	}
	for index := range first.Phases {
		if first.Phases[index] != second.Phases[index] {
			return false
		}
	}
	return true
}

func copyOverloadSpec(spec *overloadSpec) *overloadSpec {
	if spec == nil {
		return nil
	}
	copied := *spec
	copied.Phases = append([]overloadPhase(nil), spec.Phases...)
	return &copied
}

// overloadProfile is a parsed -overload-profile.
type overloadProfile struct {
	text     string
	nominal  uint64
	phases   []overloadPhase
	schedule *phasedSchedule
}

// parseOverloadProfile parses a comma-separated list of MULTIPLIERx:DURATION
// phases against the nominal rate. A multiplier is a plain decimal number; the
// rate it yields must be a whole number of messages per second. Durations
// must be whole seconds so every phase boundary coincides with a per-second
// progress observation. The profile must return to the nominal level or below
// after overload, stay there for its last phase, and give every recovery
// phase room for the recovery rule.
func parseOverloadProfile(text string, rate uint64) (*overloadProfile, error) {
	if strings.TrimSpace(text) != text || text == "" {
		return nil, errors.New("overload-profile must be a non-empty list of MULTIPLIERx:DURATION phases without surrounding spaces")
	}
	if rate == 0 || rate > maxOfferedRate {
		return nil, fmt.Errorf("overload-profile needs a nominal rate between 1 and %d", maxOfferedRate)
	}
	fields := strings.Split(text, ",")
	if len(fields) < 2 || len(fields) > overloadMaximumPhases {
		return nil, fmt.Errorf("overload-profile must have between 2 and %d phases", overloadMaximumPhases)
	}
	nominal := new(big.Rat).SetUint64(rate)
	one := big.NewRat(1, 1)
	phases := make([]overloadPhase, 0, len(fields))
	above := make([]bool, 0, len(fields))
	var total time.Duration
	for index, field := range fields {
		multiplierText, durationText, found := strings.Cut(field, ":")
		if !found {
			return nil, fmt.Errorf("overload-profile phase %d %q is not MULTIPLIERx:DURATION", index+1, field)
		}
		multiplier, err := parseOverloadMultiplier(multiplierText)
		if err != nil {
			return nil, fmt.Errorf("overload-profile phase %d: %w", index+1, err)
		}
		phaseRate := new(big.Rat).Mul(nominal, multiplier)
		if !phaseRate.IsInt() || phaseRate.Sign() <= 0 || !phaseRate.Num().IsUint64() || phaseRate.Num().Uint64() > maxOfferedRate {
			return nil, fmt.Errorf("overload-profile phase %d: %s of rate %d must be a whole rate between 1 and %d messages/s", index+1, multiplierText, rate, maxOfferedRate)
		}
		duration, err := time.ParseDuration(durationText)
		if err != nil {
			return nil, fmt.Errorf("overload-profile phase %d duration: %w", index+1, err)
		}
		if duration < time.Second || duration%time.Second != 0 {
			return nil, fmt.Errorf("overload-profile phase %d duration %s must be a positive whole number of seconds", index+1, durationText)
		}
		if duration > maxRunWindow-total {
			return nil, fmt.Errorf("overload-profile exceeds the %s run window", maxRunWindow)
		}
		total += duration
		phases = append(phases, overloadPhase{Multiplier: multiplierText, Rate: phaseRate.Num().Uint64(), Duration: duration})
		above = append(above, multiplier.Cmp(one) > 0)
	}
	if above[len(above)-1] {
		return nil, errors.New("overload-profile must end at or below 1x: its last phase is the recovery level the warm-up also runs at")
	}
	recoveries := 0
	for index := 1; index < len(phases); index++ {
		if !above[index-1] || above[index] {
			continue
		}
		recoveries++
		if phases[index].Duration < overloadMinimumRecoveryPhase {
			return nil, fmt.Errorf("overload-profile recovery phase %d must last at least %s: the %s recovery allowance, a one-second window and eight trend observations", index+1, overloadMinimumRecoveryPhase, overloadRecoveryAllowance)
		}
	}
	if recoveries == 0 {
		return nil, errors.New("overload-profile must contain a phase above 1x followed by a phase at or below 1x")
	}
	schedule, err := newPhasedSchedule(phases)
	if err != nil {
		return nil, err
	}
	return &overloadProfile{text: text, nominal: rate, phases: phases, schedule: schedule}, nil
}

// parseOverloadMultiplier accepts DIGITS[.DIGITS]x, greater than zero and at
// most overloadMaximumMultiplier, and returns it exactly.
func parseOverloadMultiplier(text string) (*big.Rat, error) {
	number, found := strings.CutSuffix(text, "x")
	if !found || number == "" {
		return nil, fmt.Errorf("multiplier %q must be a decimal number followed by x", text)
	}
	dots := 0
	for _, character := range number {
		switch {
		case character == '.':
			dots++
		case character < '0' || character > '9':
			return nil, fmt.Errorf("multiplier %q must be a decimal number followed by x", text)
		}
	}
	if dots > 1 || number[0] == '.' || number[len(number)-1] == '.' {
		return nil, fmt.Errorf("multiplier %q must be a decimal number followed by x", text)
	}
	value, ok := new(big.Rat).SetString(number)
	if !ok || value.Sign() <= 0 || value.Cmp(big.NewRat(overloadMaximumMultiplier, 1)) > 0 {
		return nil, fmt.Errorf("multiplier %q must be greater than 0x and at most %dx", text, overloadMaximumMultiplier)
	}
	return value, nil
}

func (profile *overloadProfile) duration() time.Duration {
	return profile.schedule.duration
}

func (profile *overloadProfile) expected() uint64 {
	return profile.schedule.expected
}

// warmupRate is the rate of the profile's last phase, the nominal level the
// trial returns to. The warm-up must be loss-free, so it runs there.
func (profile *overloadProfile) warmupRate() uint64 {
	return profile.phases[len(profile.phases)-1].Rate
}

func (profile *overloadProfile) spec(role string) *overloadSpec {
	spec := &overloadSpec{Profile: profile.text, Role: role, NominalRate: profile.nominal}
	if role == overloadRoleMeasurement {
		spec.Phases = append([]overloadPhase(nil), profile.phases...)
		spec.RequestDeadline = overloadRequestDeadline
	}
	return spec
}

// openLoopSchedule maps elapsed measurement time to the number of messages
// due, and each message index to its scheduled offset.
type openLoopSchedule interface {
	due(elapsed time.Duration) uint64
	offset(index uint64) time.Duration
}

// constantSchedule is the single-rate schedule of every nominal mode. Its
// arithmetic is exactly the scheduler's historical inline arithmetic.
type constantSchedule struct {
	rate uint64
}

func (schedule constantSchedule) due(elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	return uint64(elapsed)*schedule.rate/uint64(time.Second) + 1
}

func (schedule constantSchedule) offset(index uint64) time.Duration {
	return time.Duration(index * uint64(time.Second) / schedule.rate)
}

type schedulePhase struct {
	rate  uint64
	start time.Duration
	end   time.Duration
	first uint64
	count uint64
}

// phasedSchedule concatenates constant-rate phases on one clock. Within a
// phase it is the constant schedule offset by the phase's start time and first
// message index, so the scheduler switches rate exactly at each boundary.
type phasedSchedule struct {
	phases   []schedulePhase
	expected uint64
	duration time.Duration
}

func newPhasedSchedule(phases []overloadPhase) (*phasedSchedule, error) {
	if len(phases) == 0 {
		return nil, errors.New("a phased schedule needs at least one phase")
	}
	schedule := &phasedSchedule{phases: make([]schedulePhase, len(phases))}
	for index, phase := range phases {
		if phase.Rate == 0 || phase.Rate > maxOfferedRate || phase.Duration <= 0 || phase.Duration > maxRunWindow-schedule.duration {
			return nil, errors.New("phase rate or duration is out of range")
		}
		count, err := scheduledMessages(phase.Rate, phase.Duration)
		if err != nil || count == 0 || count > math.MaxUint64-schedule.expected {
			return nil, errors.New("phase schedules no messages or overflows")
		}
		schedule.phases[index] = schedulePhase{
			rate: phase.Rate, start: schedule.duration, end: schedule.duration + phase.Duration,
			first: schedule.expected, count: count,
		}
		schedule.duration += phase.Duration
		schedule.expected += count
	}
	if schedule.expected > overloadMaximumMessages {
		return nil, fmt.Errorf("overload profile schedules %d messages; the per-message outcome ledger is bounded at %d", schedule.expected, overloadMaximumMessages)
	}
	return schedule, nil
}

// phaseAt returns the phase containing elapsed, clamped to the first and last
// phases.
func (schedule *phasedSchedule) phaseAt(elapsed time.Duration) int {
	for index := len(schedule.phases) - 1; index > 0; index-- {
		if elapsed >= schedule.phases[index].start {
			return index
		}
	}
	return 0
}

func (schedule *phasedSchedule) phaseOfIndex(index uint64) int {
	for phase := len(schedule.phases) - 1; phase > 0; phase-- {
		if index >= schedule.phases[phase].first {
			return phase
		}
	}
	return 0
}

func (schedule *phasedSchedule) due(elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	if elapsed >= schedule.duration {
		return schedule.expected
	}
	phase := schedule.phases[schedule.phaseAt(elapsed)]
	local := elapsed - phase.start
	return phase.first + min(uint64(local)*phase.rate/uint64(time.Second)+1, phase.count)
}

func (schedule *phasedSchedule) offset(index uint64) time.Duration {
	phase := schedule.phases[schedule.phaseOfIndex(index)]
	return phase.start + time.Duration((index-phase.first)*uint64(time.Second)/phase.rate)
}

// scheduledBefore counts messages scheduled strictly before elapsed, the
// convention of the receiver's diagnostic series.
func (schedule *phasedSchedule) scheduledBefore(elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	if elapsed >= schedule.duration {
		return schedule.expected
	}
	phase := schedule.phases[schedule.phaseAt(elapsed)]
	local := elapsed - phase.start
	return phase.first + min(uint64(local)*phase.rate/uint64(time.Second), phase.count)
}

// validateOverloadSpec checks an overload identity carried by a run
// specification against the rest of that specification.
func validateOverloadSpec(specification runSpec) (*phasedSchedule, error) {
	spec := specification.Overload
	if spec == nil {
		return nil, nil
	}
	if specification.Mode != modeThroughput || specification.Direction != directionASPToSGP {
		return nil, errors.New("overload trials run in throughput mode from ASP to SGP only")
	}
	if specification.SSNM != nil {
		return nil, errors.New(overloadSSNMRefusal)
	}
	if spec.Profile == "" || spec.NominalRate == 0 || spec.NominalRate > maxOfferedRate {
		return nil, errors.New("overload identity needs its profile and nominal rate")
	}
	profile, err := parseOverloadProfile(spec.Profile, spec.NominalRate)
	if err != nil {
		return nil, err
	}
	switch spec.Role {
	case overloadRoleWarmup:
		if len(spec.Phases) != 0 || spec.RequestDeadline != 0 || specification.Rate != profile.warmupRate() {
			return nil, errors.New("an overload warm-up is a nominal cohort at the profile's last-phase rate and carries no phases")
		}
		return nil, nil
	case overloadRoleMeasurement:
	default:
		return nil, errors.New("overload role must be warmup or measurement")
	}
	if !sameOverloadSpec(spec, profile.spec(overloadRoleMeasurement)) {
		return nil, errors.New("overload phases do not match the profile")
	}
	if specification.Rate != spec.NominalRate || specification.Duration != profile.duration() || specification.Expected != profile.expected() {
		return nil, errors.New("overload specification rate, duration or expected count does not match the profile")
	}
	if specification.Drain < spec.RequestDeadline+overloadDrainMargin {
		return nil, errors.New("overload drain must outlast the request deadline by the drain margin")
	}
	return profile.schedule, nil
}
