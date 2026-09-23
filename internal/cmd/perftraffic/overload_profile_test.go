package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseOverloadProfileAcceptsTheApprovedShapes(testContext *testing.T) {
	for _, scenario := range []struct {
		text     string
		rate     uint64
		phases   []overloadPhase
		expected uint64
		warmup   uint64
	}{
		{
			text: "2x:60s,0.5x:60s", rate: 40_000,
			phases:   []overloadPhase{{Multiplier: "2x", Rate: 80_000, Duration: time.Minute}, {Multiplier: "0.5x", Rate: 20_000, Duration: time.Minute}},
			expected: 6_000_000, warmup: 20_000,
		},
		{
			text: "2x:15s,0.5x:15s", rate: 40_000,
			phases:   []overloadPhase{{Multiplier: "2x", Rate: 80_000, Duration: 15 * time.Second}, {Multiplier: "0.5x", Rate: 20_000, Duration: 15 * time.Second}},
			expected: 1_500_000, warmup: 20_000,
		},
		{
			text: "1x:10s,1.25x:5s,1x:12s,3x:2s,0.25x:20s", rate: 1_000,
			phases: []overloadPhase{
				{Multiplier: "1x", Rate: 1_000, Duration: 10 * time.Second}, {Multiplier: "1.25x", Rate: 1_250, Duration: 5 * time.Second},
				{Multiplier: "1x", Rate: 1_000, Duration: 12 * time.Second}, {Multiplier: "3x", Rate: 3_000, Duration: 2 * time.Second},
				{Multiplier: "0.25x", Rate: 250, Duration: 20 * time.Second},
			},
			expected: 10_000 + 6_250 + 12_000 + 6_000 + 5_000, warmup: 250,
		},
	} {
		profile, err := parseOverloadProfile(scenario.text, scenario.rate)
		if err != nil {
			testContext.Fatalf("%s: %v", scenario.text, err)
		}
		if len(profile.phases) != len(scenario.phases) {
			testContext.Fatalf("%s: phases %+v", scenario.text, profile.phases)
		}
		var duration time.Duration
		for index, phase := range profile.phases {
			if phase != scenario.phases[index] {
				testContext.Fatalf("%s: phase %d = %+v, want %+v", scenario.text, index, phase, scenario.phases[index])
			}
			duration += phase.Duration
		}
		if profile.expected() != scenario.expected || profile.duration() != duration || profile.warmupRate() != scenario.warmup {
			testContext.Fatalf("%s: expected %d duration %s warmup %d", scenario.text, profile.expected(), profile.duration(), profile.warmupRate())
		}
		spec := profile.spec(overloadRoleMeasurement)
		if spec.Profile != scenario.text || spec.NominalRate != scenario.rate || spec.RequestDeadline != overloadRequestDeadline || len(spec.Phases) != len(scenario.phases) {
			testContext.Fatalf("%s: measurement spec %+v", scenario.text, spec)
		}
		if warmup := profile.spec(overloadRoleWarmup); len(warmup.Phases) != 0 || warmup.RequestDeadline != 0 || warmup.Role != overloadRoleWarmup {
			testContext.Fatalf("%s: warm-up spec %+v", scenario.text, warmup)
		}
	}
}

func TestParseOverloadProfileRejectsMalformedAndUnsafeProfiles(testContext *testing.T) {
	for _, scenario := range []struct {
		text string
		rate uint64
		want string
	}{
		{text: "", rate: 40_000, want: "non-empty"},
		{text: " 2x:60s,0.5x:60s", rate: 40_000, want: "surrounding spaces"},
		{text: "2x:60s", rate: 40_000, want: "between 2 and 8 phases"},
		{text: "2x:60s,0.5x:60s,2x:60s,0.5x:60s,2x:60s,0.5x:60s,2x:60s,0.5x:60s,0.5x:60s", rate: 1_000, want: "between 2 and 8 phases"},
		{text: "2:60s,0.5x:60s", rate: 40_000, want: "decimal number followed by x"},
		{text: "2x60s,0.5x:60s", rate: 40_000, want: "MULTIPLIERx:DURATION"},
		{text: "-1x:60s,0.5x:60s", rate: 40_000, want: "decimal number followed by x"},
		{text: "0x:60s,0.5x:60s", rate: 40_000, want: "greater than 0x"},
		{text: ".5x:60s,2x:60s", rate: 40_000, want: "decimal number followed by x"},
		{text: "2.x:60s,0.5x:60s", rate: 40_000, want: "decimal number followed by x"},
		{text: "1e1x:60s,0.5x:60s", rate: 40_000, want: "decimal number followed by x"},
		{text: "1/2x:60s,2x:60s", rate: 40_000, want: "decimal number followed by x"},
		{text: "17x:60s,0.5x:60s", rate: 1_000, want: "at most 16x"},
		{text: "2x:60s,0.3x:60s", rate: 40_001, want: "whole rate"},
		{text: "16x:60s,0.5x:60s", rate: 100_000, want: "whole rate"},
		{text: "2x:1500ms,0.5x:60s", rate: 40_000, want: "whole number of seconds"},
		{text: "2x:0s,0.5x:60s", rate: 40_000, want: "whole number of seconds"},
		{text: "2x:sixty,0.5x:60s", rate: 40_000, want: "duration"},
		{text: "2x:60s,0.5x:11s", rate: 40_000, want: "at least 12s"},
		{text: "0.5x:60s,2x:60s", rate: 40_000, want: "end at or below 1x"},
		{text: "0.5x:60s,1x:60s", rate: 40_000, want: "phase above 1x followed"},
		{text: "2x:300s,0.5x:301s", rate: 1_000, want: "run window"},
		{text: "2x:300s,0.5x:300s", rate: 100_000, want: "per-message outcome ledger"},
		{text: "2x:60s,0.5x:60s", rate: 0, want: "nominal rate"},
	} {
		_, err := parseOverloadProfile(scenario.text, scenario.rate)
		if err == nil || !strings.Contains(err.Error(), scenario.want) {
			testContext.Errorf("%q at %d: err = %v, want %q", scenario.text, scenario.rate, err, scenario.want)
		}
	}
}

func TestOverloadProfileFlagConfiguresOnlyTheThroughputSender(testContext *testing.T) {
	base := []string{"-role=asp", "-transport=dial", "-peer-control=http://peer:8080", "-cohort=overload", "-rate=40000", "-associations=8", "-payload=mix"}
	config, err := parseConfig(append(base, "-overload-profile=2x:15s,0.5x:15s", "-warmup=5s", "-same-host-clock"))
	if err != nil {
		testContext.Fatal(err)
	}
	if config.overload == nil || config.Duration != 30*time.Second || config.Drain != 3*time.Second || config.Rate != 40_000 {
		testContext.Fatalf("overload config = %+v", config)
	}
	config, err = parseConfig(append(base, "-overload-profile=2x:15s,0.5x:15s", "-duration=30s", "-drain=5s"))
	if err != nil || config.Duration != 30*time.Second || config.Drain != 5*time.Second {
		testContext.Fatalf("matching duration and longer drain: %+v, %v", config, err)
	}
	plain, err := parseConfig(base)
	if err != nil || plain.overload != nil || plain.OverloadProfile != "" || plain.Duration != 2*time.Minute || plain.Drain != 2*time.Second {
		testContext.Fatalf("default config changed: %+v, %v", plain, err)
	}
	for _, scenario := range []struct {
		arguments []string
		want      string
	}{
		{arguments: append(base, "-overload-profile=2x:15s,0.5x:15s", "-duration=20s"), want: "contradicts"},
		{arguments: append(base, "-overload-profile=2x:15s,0.5x:15s", "-mode=bidirectional", "-control-url=http://asp:8081"), want: "throughput mode only"},
		{arguments: append(base, "-overload-profile=2x:15s,0.5x:15s", "-mode=echo"), want: "throughput mode only"},
		{arguments: []string{"-role=sgp", "-overload-profile=2x:15s,0.5x:15s"}, want: "ASP sender flag"},
		{arguments: append(base, "-overload-profile=2x:15s"), want: "between 2 and 8 phases"},
		{arguments: append(base, "-overload-profile=2x:300s,0.5x:299s", "-warmup=5s"), want: "exceed"},
	} {
		if _, err := parseConfig(scenario.arguments); err == nil || !strings.Contains(err.Error(), scenario.want) {
			testContext.Errorf("%v: err = %v, want %q", scenario.arguments, err, scenario.want)
		}
	}
}

// The constant schedule must reproduce the scheduler's historical inline
// arithmetic exactly, so every nominal mode is unchanged.
func TestConstantScheduleMatchesTheHistoricalArithmetic(testContext *testing.T) {
	random := rand.New(rand.NewSource(1))
	for trial := 0; trial < 100_000; trial++ {
		rate := uint64(random.Int63n(maxOfferedRate)) + 1
		elapsed := time.Duration(random.Int63n(int64(maxRunWindow))) - time.Second
		index := uint64(random.Int63n(int64(rate) * 600))
		wantDue := uint64(0)
		if elapsed > 0 {
			wantDue = uint64(elapsed)*rate/uint64(time.Second) + 1
		}
		wantOffset := time.Duration(index * uint64(time.Second) / rate)
		schedule := constantSchedule{rate: rate}
		if schedule.due(elapsed) != wantDue || schedule.offset(index) != wantOffset {
			testContext.Fatalf("rate %d elapsed %s index %d: due %d/%d offset %s/%s", rate, elapsed, index, schedule.due(elapsed), wantDue, schedule.offset(index), wantOffset)
		}
	}
}

func TestPhasedScheduleSwitchesRateAtEachBoundary(testContext *testing.T) {
	schedule, err := newPhasedSchedule([]overloadPhase{{Rate: 1_000, Duration: 2 * time.Second}, {Rate: 250, Duration: 2 * time.Second}, {Rate: 4_000, Duration: time.Second}})
	if err != nil {
		testContext.Fatal(err)
	}
	if schedule.expected != 2_000+500+4_000 || schedule.duration != 5*time.Second {
		testContext.Fatalf("schedule = %+v", schedule)
	}
	for _, check := range []struct {
		elapsed time.Duration
		due     uint64
		before  uint64
		phase   int
	}{
		{elapsed: 0, due: 0, before: 0, phase: 0},
		{elapsed: 1, due: 1, before: 0, phase: 0},
		{elapsed: time.Second, due: 1_001, before: 1_000, phase: 0},
		{elapsed: 2*time.Second - 1, due: 2_000, before: 1_999, phase: 0},
		{elapsed: 2 * time.Second, due: 2_001, before: 2_000, phase: 1},
		{elapsed: 2*time.Second + 3*time.Millisecond, due: 2_001, before: 2_000, phase: 1},
		{elapsed: 2*time.Second + 4*time.Millisecond, due: 2_002, before: 2_001, phase: 1},
		{elapsed: 4*time.Second - 1, due: 2_500, before: 2_499, phase: 1},
		{elapsed: 4 * time.Second, due: 2_501, before: 2_500, phase: 2},
		{elapsed: 4*time.Second + 250*time.Microsecond, due: 2_502, before: 2_501, phase: 2},
		{elapsed: 5 * time.Second, due: 6_500, before: 6_500, phase: 2},
		{elapsed: time.Hour, due: 6_500, before: 6_500, phase: 2},
	} {
		if due := schedule.due(check.elapsed); due != check.due {
			testContext.Errorf("due(%s) = %d, want %d", check.elapsed, due, check.due)
		}
		if before := schedule.scheduledBefore(check.elapsed); before != check.before {
			testContext.Errorf("scheduledBefore(%s) = %d, want %d", check.elapsed, before, check.before)
		}
		if phase := schedule.phaseAt(check.elapsed); phase != check.phase {
			testContext.Errorf("phaseAt(%s) = %d, want %d", check.elapsed, phase, check.phase)
		}
	}
	for _, check := range []struct {
		index  uint64
		offset time.Duration
		phase  int
	}{
		{index: 0, offset: 0, phase: 0}, {index: 1_999, offset: 1_999 * time.Millisecond, phase: 0},
		{index: 2_000, offset: 2 * time.Second, phase: 1}, {index: 2_001, offset: 2*time.Second + 4*time.Millisecond, phase: 1},
		{index: 2_499, offset: 2*time.Second + 499*4*time.Millisecond, phase: 1},
		{index: 2_500, offset: 4 * time.Second, phase: 2}, {index: 6_499, offset: 4*time.Second + 3_999*250*time.Microsecond, phase: 2},
	} {
		if offset := schedule.offset(check.index); offset != check.offset {
			testContext.Errorf("offset(%d) = %s, want %s", check.index, offset, check.offset)
		}
		if phase := schedule.phaseOfIndex(check.index); phase != check.phase {
			testContext.Errorf("phaseOfIndex(%d) = %d, want %d", check.index, phase, check.phase)
		}
		// Every message is due exactly at its own offset and not before.
		if offset := schedule.offset(check.index); offset > 0 && (schedule.due(offset) <= check.index || schedule.due(offset-1) > check.index) {
			testContext.Errorf("message %d at %s is due %d/%d", check.index, offset, schedule.due(offset-1), schedule.due(offset))
		}
	}
}

// steppingMeasurementClock is a shared clock that advances a fixed step on
// every read, so the scheduler runs against simulated time.
type steppingMeasurementClock struct {
	mutex   sync.Mutex
	now     int64
	step    int64
	current int64
}

func (clock *steppingMeasurementClock) Now() (int64, error) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.now += clock.step
	clock.current = clock.now
	return clock.now, nil
}

func (clock *steppingMeasurementClock) Domain() (sharedClockDomain, error) {
	return sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test", TimeNamespace: "time:[1]", Resolution: 1}, nil
}

func (clock *steppingMeasurementClock) last() time.Duration {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return time.Duration(clock.current)
}

func TestOverloadSchedulerSwitchesRatesOnTheSharedClock(testContext *testing.T) {
	phases := []overloadPhase{{Rate: 100_000, Duration: 20 * time.Millisecond}, {Rate: 25_000, Duration: 20 * time.Millisecond}}
	schedule, err := newPhasedSchedule(phases)
	if err != nil {
		testContext.Fatal(err)
	}
	start := int64(time.Second)
	source := &steppingMeasurementClock{now: start - int64(time.Millisecond), step: int64(7 * time.Microsecond)}
	domain, _ := source.Domain()
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: start, End: start + int64(schedule.duration)}}
	counters := newSenderCounters(int(schedule.expected))
	counters.overload = newOverloadCounters(schedule)
	queue := make(chan sendJob, schedule.expected)
	maxima := dispatchOverload(context.Background(), commandConfig{Seed: 3, Workload: workloadMix}, "phased", schedule, time.Now(), []chan sendJob{queue}, counters, clock)
	close(queue)
	perPhase := make([]uint64, len(phases))
	var previous time.Duration
	var index uint64
	for job := range queue {
		if globalIndex(job.identity) != index || job.offset != schedule.offset(index) || job.clock != clock {
			testContext.Fatalf("job %d = %+v", index, job)
		}
		phase := schedule.phaseOfIndex(index)
		perPhase[phase]++
		if index > 0 {
			spacing := job.offset - previous
			want := time.Second / time.Duration(schedule.phases[schedule.phaseOfIndex(index-1)].rate)
			if index == schedule.phases[1].first {
				want = schedule.phases[1].start - previous
			}
			if spacing != want {
				testContext.Fatalf("message %d spacing %s, want %s", index, spacing, want)
			}
		}
		previous = job.offset
		index++
	}
	if perPhase[0] != 2_000 || perPhase[1] != 500 || index != schedule.expected || maxima[0] != int(schedule.expected) {
		testContext.Fatalf("per-phase %v total %d maxima %v", perPhase, index, maxima)
	}
	if elapsed := source.last() - time.Duration(start); elapsed < schedule.duration {
		testContext.Fatalf("scheduler returned at %s, before the window end", elapsed)
	}
	totals := counters.overload.totals()
	if counters.scheduled != schedule.expected || totals.Offered != schedule.expected || counters.overload.phases[0].Offered != 2_000 ||
		counters.overload.phases[1].Offered != 500 || counters.capped != 0 || counters.fatal != "" {
		testContext.Fatalf("counters scheduled %d capped %d fatal %q phases %+v", counters.scheduled, counters.capped, counters.fatal, counters.overload.phases)
	}
}

// Under a cap the scheduler refuses rather than delays: the offered schedule
// is unchanged and every refusal is counted in the phase it was scheduled in.
func TestOverloadSchedulerCountsCapRefusalsByPhase(testContext *testing.T) {
	schedule, err := newPhasedSchedule([]overloadPhase{{Rate: 100_000, Duration: 10 * time.Millisecond}, {Rate: 10_000, Duration: 10 * time.Millisecond}})
	if err != nil {
		testContext.Fatal(err)
	}
	start := int64(time.Second)
	source := &steppingMeasurementClock{now: start - int64(time.Millisecond), step: int64(5 * time.Microsecond)}
	domain, _ := source.Domain()
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: start, End: start + int64(schedule.duration)}}
	counters := newSenderCounters(10)
	counters.overload = newOverloadCounters(schedule)
	full := make(chan sendJob, 4)
	spare := make(chan sendJob, 100)
	dispatchOverload(context.Background(), commandConfig{Seed: 3, Workload: workload128}, "capped", schedule, time.Now(), []chan sendJob{full, spare}, counters, clock)
	first, second := counters.overload.phases[0], counters.overload.phases[1]
	queued := uint64(len(full) + len(spare))
	refused := first.CapRefusedOutstanding + first.CapRefusedQueue + second.CapRefusedOutstanding + second.CapRefusedQueue
	if first.Offered != 1_000 || second.Offered != 100 || queued != 10 || len(full) != 4 || refused != 1_090 ||
		counters.capped != refused || counters.outstanding != 10 || first.CapRefusedQueue == 0 || first.CapRefusedOutstanding == 0 {
		testContext.Fatalf("phases %+v %+v queued %d capped %d outstanding %d", first, second, queued, counters.capped, counters.outstanding)
	}
	var notAdmitted uint64
	for _, count := range counters.overload.notAdmitted {
		notAdmitted += count
	}
	if notAdmitted != refused || counters.overload.maxOutstanding != 10 {
		testContext.Fatalf("not admitted %d, max outstanding %d", notAdmitted, counters.overload.maxOutstanding)
	}
}

func TestOverloadSpecificationRoundTripsAndCompares(testContext *testing.T) {
	profile, err := parseOverloadProfile("2x:15s,0.5x:15s", 40_000)
	if err != nil {
		testContext.Fatal(err)
	}
	specification := runSpec{
		Cohort: "overload", Seed: 1, Associations: 8, Expected: profile.expected(), Duration: profile.duration(),
		Drain: 3 * time.Second, Rate: 40_000, Outstanding: maxOutstanding, Payload: workloadMix,
		Mode: modeThroughput, Direction: directionASPToSGP, Initiation: initiationASPDial,
		Overload: profile.spec(overloadRoleMeasurement),
	}
	encoded, err := json.Marshal(specification)
	if err != nil {
		testContext.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var decoded runSpec
	if err := decoder.Decode(&decoded); err != nil {
		testContext.Fatal(err)
	}
	if !sameRunSpec(decoded, specification) || !sameRunSpec(copyRunSpec(specification), specification) {
		testContext.Fatalf("round trip changed the specification: %+v", decoded)
	}
	copied := copyRunSpec(specification)
	copied.Overload.Phases[0].Rate++
	if sameRunSpec(copied, specification) || specification.Overload.Phases[0].Rate != 80_000 {
		testContext.Fatal("copy shares or ignores overload phases")
	}
	nominal := specification
	nominal.Overload = nil
	if sameRunSpec(nominal, specification) {
		testContext.Fatal("a nominal specification compared equal to an overload one")
	}
	if schedule, err := validateOverloadSpec(specification); err != nil || schedule == nil || schedule.expected != profile.expected() {
		testContext.Fatalf("validate: %v", err)
	}
	if expected, err := specExpected(specification); err != nil || expected != 1_500_000 {
		testContext.Fatalf("specExpected = %d, %v", expected, err)
	}
	for name, mutate := range map[string]func(*runSpec){
		"echo":            func(spec *runSpec) { spec.Mode = modeEcho },
		"expected":        func(spec *runSpec) { spec.Expected++ },
		"duration":        func(spec *runSpec) { spec.Duration += time.Second },
		"rate":            func(spec *runSpec) { spec.Rate = 20_000 },
		"drain":           func(spec *runSpec) { spec.Drain = 2 * time.Second },
		"phase":           func(spec *runSpec) { spec.Overload.Phases[1].Rate = 30_000 },
		"role":            func(spec *runSpec) { spec.Overload.Role = "probe" },
		"deadline":        func(spec *runSpec) { spec.Overload.RequestDeadline = time.Second },
		"warmup-phases":   func(spec *runSpec) { spec.Overload.Role = overloadRoleWarmup },
		"profile":         func(spec *runSpec) { spec.Overload.Profile = "2x:15s,0.5x:16s" },
		"nominal":         func(spec *runSpec) { spec.Overload.NominalRate = 0 },
		"sgp-to-asp":      func(spec *runSpec) { spec.Direction = directionSGPToASP },
		"routed":          func(spec *runSpec) { spec.Mode = modeRouted },
		"routed-direct":   func(spec *runSpec) { spec.Mode = modeRoutedDirect },
		"bidirectional":   func(spec *runSpec) { spec.Mode = modeBidirectional },
		"profile-invalid": func(spec *runSpec) { spec.Overload.Profile = "2x:15s" },
	} {
		mutated := copyRunSpec(specification)
		mutate(&mutated)
		if _, err := validateOverloadSpec(mutated); err == nil {
			testContext.Errorf("%s: invalid overload specification accepted", name)
		}
	}
	warmup := runSpec{
		Cohort: "overload-warmup", Seed: 1, Associations: 8, Expected: 100_000, Duration: 5 * time.Second,
		Drain: 3 * time.Second, Rate: 20_000, Outstanding: maxOutstanding, Payload: workloadMix,
		Mode: modeThroughput, Direction: directionASPToSGP, Overload: profile.spec(overloadRoleWarmup),
	}
	if schedule, err := validateOverloadSpec(warmup); err != nil || schedule != nil || warmup.overloadMeasurement() {
		testContext.Fatalf("warm-up identity: %v", err)
	}
	warmup.Rate = 40_000
	if _, err := validateOverloadSpec(warmup); err == nil {
		testContext.Fatal("a warm-up at the overload rate was accepted")
	}
}

// Nominal records must serialize exactly as before: no overload member in the
// specification, the record, a progress snapshot or a progress observation.
func TestNominalRecordsCarryNoOverloadMembers(testContext *testing.T) {
	observation := progressObservation{Snapshot: &receiverProgress{Spec: runSpec{Cohort: "nominal"}}}
	record := runRecord{Side: "sender", Spec: runSpec{Cohort: "nominal"}, ProgressObservations: []progressObservation{observation}}
	record.evaluate()
	encoded, err := json.Marshal(record)
	if err != nil {
		testContext.Fatal(err)
	}
	for _, member := range []string{`"overload"`, `"sender_before"`, `"sender_after"`} {
		if strings.Contains(string(encoded), member) {
			testContext.Fatalf("nominal record carries %s: %s", member, encoded)
		}
	}
}
