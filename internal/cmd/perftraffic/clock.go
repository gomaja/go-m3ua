package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const sharedClockLead = 2 * time.Second

const sharedClockWatchdogBudget = time.Millisecond

type sharedClockDomain struct {
	Clock         string `json:"clock"`
	BootID        string `json:"boot_id"`
	TimeNamespace string `json:"time_namespace"`
	Resolution    int64  `json:"resolution_ns"`
}

// monotonicOffsetIdentity is the time_namespace identity of a process whose
// CLOCK_MONOTONIC is offset by seconds and nanoseconds from its boot's clock.
func monotonicOffsetIdentity(seconds, nanoseconds int64) string {
	return fmt.Sprintf("monotonic-offset:%d.%09d", seconds, nanoseconds)
}

// parseMonotonicOffset reads the monotonic line of a
// /proc/<pid>/timens_offsets file (time_namespaces(7)): "monotonic <secs>
// <nanosecs>". Missing, repeated or malformed monotonic entries are errors.
func parseMonotonicOffset(offsets string) (string, error) {
	identity := ""
	for _, line := range strings.Split(offsets, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "monotonic" {
			continue
		}
		if identity != "" || len(fields) != 3 {
			return "", errors.New("malformed monotonic time-namespace offset")
		}
		seconds, secondsErr := strconv.ParseInt(fields[1], 10, 64)
		nanoseconds, nanosecondsErr := strconv.ParseInt(fields[2], 10, 64)
		if secondsErr != nil || nanosecondsErr != nil || nanoseconds < 0 || nanoseconds >= int64(time.Second) {
			return "", errors.New("malformed monotonic time-namespace offset")
		}
		identity = monotonicOffsetIdentity(seconds, nanoseconds)
	}
	if identity == "" {
		return "", errors.New("time-namespace offsets name no monotonic clock")
	}
	return identity, nil
}

func (domain sharedClockDomain) valid() bool {
	return domain.Clock == "CLOCK_MONOTONIC" && domain.BootID != "" && domain.TimeNamespace != "" && domain.Resolution > 0 && domain.Resolution <= int64(time.Second)
}

type sharedClockWindow struct {
	Domain sharedClockDomain `json:"domain"`
	Start  int64             `json:"start_ns"`
	End    int64             `json:"end_ns"`
}

func (window sharedClockWindow) valid(duration time.Duration) bool {
	return window.Domain.valid() && window.Start > window.Domain.Resolution && window.End > window.Start && window.End <= math.MaxInt64-window.Domain.Resolution && window.End-window.Start == int64(duration)
}

type measurementClock interface {
	Now() (int64, error)
	Domain() (sharedClockDomain, error)
}

type sharedClockSnapshot struct {
	Domain           sharedClockDomain `json:"domain"`
	Captured         int64             `json:"captured_ns"`
	MeasurementLower uint64            `json:"measurement_lower"`
	MeasurementUpper uint64            `json:"measurement_upper"`
}

type sharedClockEvidence struct {
	Before   sharedClockDomain    `json:"before"`
	After    sharedClockDomain    `json:"after"`
	Verified bool                 `json:"verified"`
	Watchdog *sharedClockWatchdog `json:"watchdog,omitempty"`
}

type sharedClockWatchdog struct {
	Before          int64 `json:"before_ns"`
	After           int64 `json:"after_ns"`
	Target          int64 `json:"target_ns"`
	MaximumLateness int64 `json:"maximum_lateness_ns"`
	Budget          int64 `json:"budget_ns"`
}

type sharedClockRequest struct {
	Before int64 `json:"before_ns"`
	After  int64 `json:"after_ns"`
}

type sharedRunClock struct {
	source measurementClock
	window sharedClockWindow
	drain  time.Duration
}

func (window sharedClockWindow) validDrain(drain time.Duration) bool {
	return drain >= 0 && drain <= maxRunWindow && window.End > 0 && window.Domain.valid() && window.End <= math.MaxInt64-int64(drain)-window.Domain.Resolution
}

func (clock *sharedRunClock) watchdog(goNow func() time.Time) (time.Time, *sharedClockWatchdog, error) {
	if !clock.window.validDrain(clock.drain) {
		return time.Time{}, nil, errors.New("invalid shared drain deadline")
	}
	before, err := clock.source.Now()
	if err != nil {
		return time.Time{}, nil, err
	}
	local := goNow()
	after, err := clock.source.Now()
	resolution := clock.window.Domain.Resolution
	target := clock.window.End + int64(clock.drain)
	if err != nil || before <= 0 || after < before || after >= target || after-before > int64(sharedClockWatchdogBudget)-2*resolution {
		return time.Time{}, nil, errors.New("shared clock watchdog translation exceeds its uncertainty budget or deadline")
	}
	evidence := &sharedClockWatchdog{Before: before, After: after, Target: target, MaximumLateness: after - before + 2*resolution, Budget: int64(sharedClockWatchdogBudget)}
	return local.Add(time.Duration(target - before + resolution)), evidence, nil
}

func (clock *sharedRunClock) waitUntil(ctx context.Context, target int64) error {
	var previous int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now, err := clock.source.Now()
		if err != nil || now <= 0 || now < previous {
			return errors.New("shared wait clock failed or regressed")
		}
		if now >= target {
			return nil
		}
		previous = now
		timer := time.NewTimer(time.Duration(target - now))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (clock *sharedRunClock) withinDrain(dispatched time.Duration) error {
	now, err := clock.source.Now()
	if err != nil || !clock.window.validDrain(clock.drain) || now < clock.window.Start || time.Duration(now-clock.window.Start) < dispatched || now > clock.window.End+int64(clock.drain)-clock.window.Domain.Resolution {
		return errors.New("shared clock completion is outside the drain deadline")
	}
	return nil
}

func (job sendJob) dispatchDelay() (time.Duration, error) {
	if job.clock == nil {
		return time.Since(job.scheduled), nil
	}
	elapsed, err := job.clock.elapsed()
	if err != nil || elapsed < job.offset {
		return 0, errors.New("shared dispatch clock failed or precedes schedule")
	}
	return elapsed - job.offset, nil
}

func sameRunSpec(first, second runSpec) bool {
	firstClock, secondClock := first.Clock, second.Clock
	firstSSNM, secondSSNM := first.SSNM, second.SSNM
	firstFailure, secondFailure := first.SGPFailure, second.SGPFailure
	firstReferences, secondReferences := first.RouteReferences, second.RouteReferences
	first.Clock, second.Clock = nil, nil
	first.SSNM, second.SSNM = nil, nil
	first.SGPFailure, second.SGPFailure = nil, nil
	first.RouteReferences, second.RouteReferences = nil, nil
	if first != second {
		return false
	}
	return samePointee(firstClock, secondClock) && samePointee(firstSSNM, secondSSNM) && samePointee(firstFailure, secondFailure) &&
		samePointee(firstReferences, secondReferences)
}

// samePointee compares two optional values: both absent, or both present and
// equal.
func samePointee[T comparable](first, second *T) bool {
	if first == nil || second == nil {
		return first == second
	}
	return *first == *second
}

func copyRunSpec(specification runSpec) runSpec {
	if specification.Clock != nil {
		window := *specification.Clock
		specification.Clock = &window
	}
	if specification.SSNM != nil {
		workload := *specification.SSNM
		specification.SSNM = &workload
	}
	if specification.SGPFailure != nil {
		failure := *specification.SGPFailure
		specification.SGPFailure = &failure
	}
	if specification.RouteReferences != nil {
		references := *specification.RouteReferences
		specification.RouteReferences = &references
	}
	return specification
}

func readPeerClock(ctx context.Context, baseURL string, clock measurementClock) (sharedClockSnapshot, error) {
	before, err := clock.Now()
	if err != nil {
		return sharedClockSnapshot{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/clock", nil)
	if err != nil {
		return sharedClockSnapshot{}, err
	}
	response, err := fixtureHTTPClient.Do(request)
	if err != nil {
		return sharedClockSnapshot{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return sharedClockSnapshot{}, fmt.Errorf("peer clock: %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		return sharedClockSnapshot{}, errors.New("peer clock response failed or exceeds 4 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var snapshot sharedClockSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return sharedClockSnapshot{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return sharedClockSnapshot{}, errors.New("peer clock contains trailing data")
	}
	after, err := clock.Now()
	if err != nil {
		return sharedClockSnapshot{}, err
	}
	domain, err := clock.Domain()
	if err != nil || !domain.valid() || snapshot.Domain != domain || !clockEnvelopeContains(before, after, snapshot.Captured, domain.Resolution) {
		return sharedClockSnapshot{}, errors.New("peer clock domain or request envelope mismatch")
	}
	return snapshot, nil
}

func clockEnvelopeContains(before, after, captured, resolution int64) bool {
	return resolution > 0 && before >= resolution && after >= before && after <= math.MaxInt64-resolution && captured >= before-resolution && captured <= after+resolution
}

func prepareSharedRunClock(ctx context.Context, config commandConfig, specification *runSpec) (*sharedRunClock, error) {
	if !config.SameHostClock {
		return nil, nil
	}
	if specification.Mode == modeEcho {
		return nil, errors.New("shared clock does not support echo measurements")
	}
	source, err := newMeasurementClock()
	if err != nil {
		return nil, err
	}
	peer, err := readPeerClock(ctx, config.PeerControl, source)
	if err != nil {
		return nil, err
	}
	now, err := source.Now()
	if err != nil || now <= 0 || now > math.MaxInt64-int64(sharedClockLead)-int64(specification.Duration)-peer.Domain.Resolution {
		return nil, errors.New("shared clock cannot establish a measurement window")
	}
	window := sharedClockWindow{Domain: peer.Domain, Start: now + int64(sharedClockLead), End: now + int64(sharedClockLead) + int64(specification.Duration)}
	if config.clockWindow != nil {
		window = *config.clockWindow
	}
	if !window.valid(specification.Duration) || !window.validDrain(specification.Drain) || window.Domain != peer.Domain || now+window.Domain.Resolution >= window.Start || window.Start-now > int64(sharedClockLead) {
		return nil, errors.New("shared clock window is invalid or already started")
	}
	specification.Clock = &window
	return &sharedRunClock{source: source, window: window, drain: specification.Drain}, nil
}

func (clock *sharedRunClock) elapsed() (time.Duration, error) {
	now, err := clock.source.Now()
	if err != nil || now <= 0 {
		return 0, errors.New("shared monotonic clock read failed")
	}
	return time.Duration(now - clock.window.Start), nil
}

func (control *receiverControl) enableSharedClock(enabled bool) {
	if !enabled {
		return
	}
	clock, err := newMeasurementClock()
	if err != nil {
		control.fatal = err.Error()
		return
	}
	control.clock = clock
}

func (control *receiverControl) clockSnapshot() (sharedClockSnapshot, error) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.clock == nil {
		return sharedClockSnapshot{}, errors.New("same-host clock mode is not enabled")
	}
	domain, err := control.clock.Domain()
	if err != nil || !domain.valid() {
		return sharedClockSnapshot{}, errors.New("cannot verify local clock domain")
	}
	now, err := control.clock.Now()
	if err != nil || now <= 0 || now > math.MaxInt64-domain.Resolution {
		return sharedClockSnapshot{}, errors.New("cannot read local monotonic clock")
	}
	return sharedClockSnapshot{Domain: domain, Captured: now}, nil
}

func (control *receiverControl) sharedNowLocked() (int64, error) {
	now, err := control.clock.Now()
	if err != nil || now <= 0 || now < control.lastClock || now > math.MaxInt64-control.spec.Clock.Domain.Resolution {
		control.fatal = "shared monotonic clock failed or regressed"
		return 0, errors.New(control.fatal)
	}
	control.lastClock = now
	return now, nil
}

func (control *receiverControl) classifySharedDeliveryLocked(now int64) {
	window := control.spec.Clock
	resolution := window.Domain.Resolution
	if now-resolution >= window.Start && now+resolution < window.End {
		control.measurementLower++
	}
	if now+resolution >= window.Start && now-resolution < window.End {
		control.measurementUpper++
	}
	if now >= window.Start && now < window.End {
		control.uniqueMeasurement++
	} else {
		control.uniqueDrain++
	}
	if now+resolution < window.Start {
		control.fatal = "validated DATA arrived before the shared measurement window"
	}
}
