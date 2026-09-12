package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseCPUStat(testContext *testing.T) {
	statistics, err := parseCPUStat(strings.NewReader("usage_usec 123\nuser_usec 100\nsystem_usec 23\nnr_periods 4\nnr_throttled 1\nthrottled_usec 9\n"))
	if err != nil {
		testContext.Fatalf("parseCPUStat: %v", err)
	}
	if statistics["usage_usec"] != 123 || statistics["nr_throttled"] != 1 {
		testContext.Fatalf("statistics = %#v", statistics)
	}
}

func TestBoundedHistogramPercentiles(testContext *testing.T) {
	histogram := newDurationHistogram()
	for _, duration := range []time.Duration{time.Microsecond, 10 * time.Microsecond, 100 * time.Microsecond, time.Millisecond} {
		histogram.record(duration)
	}
	percentiles := histogram.percentiles()
	if percentiles.Count != 4 || percentiles.P50 < 10*time.Microsecond || percentiles.P99 < time.Millisecond {
		testContext.Fatalf("percentiles = %+v", percentiles)
	}
	if histogram.storageBytes() > 1024 {
		testContext.Fatalf("histogram storage = %d bytes", histogram.storageBytes())
	}
}

func TestVerdictIsInconclusiveWhenWholeProcessCPUUnavailable(testContext *testing.T) {
	record := runRecord{
		Expected: 1,
		Delivery: deliveryResult{UniqueMeasurement: 1, Unique: 1},
		CPU:      CPUObservation{Scope: wholeProcessScope, Error: "cpu.stat unavailable"},
	}
	record.evaluate()
	if record.Verdict != verdictInconclusive {
		testContext.Fatalf("verdict = %q, want inconclusive", record.Verdict)
	}
}

func TestVerdictNeverClaimsIndependentPeerOrLibraryOnlyAccounting(testContext *testing.T) {
	record := runRecord{
		Expected: 1,
		Delivery: deliveryResult{UniqueMeasurement: 1, Unique: 1},
		CPU: CPUObservation{
			Scope:  wholeProcessScope,
			Before: map[string]uint64{"usage_usec": 10, "nr_throttled": 0},
			After:  map[string]uint64{"usage_usec": 20, "nr_throttled": 0},
		},
		Allocations: AllocationObservation{Scope: wholeProcessScope},
	}
	record.evaluate()
	if record.Verdict != verdictInconclusive || record.FixtureVerdict != verdictPass || record.CapacityVerdict != "unavailable" || record.IndependentPeer || record.AcceptanceScope != baselineFixtureScope {
		testContext.Fatalf("record = %+v", record)
	}
}

func TestVerdictIsInconclusiveForMissingOrRegressedCPUUsage(testContext *testing.T) {
	testCases := []CPUObservation{
		{Scope: wholeProcessScope, Before: map[string]uint64{}, After: map[string]uint64{}},
		{Scope: wholeProcessScope, Before: map[string]uint64{"usage_usec": 20, "nr_throttled": 0}, After: map[string]uint64{"usage_usec": 10, "nr_throttled": 0}},
		{Scope: wholeProcessScope, Before: map[string]uint64{"usage_usec": 20, "nr_throttled": 0}, After: map[string]uint64{"usage_usec": 20, "nr_throttled": 0}},
		{Scope: wholeProcessScope, Before: map[string]uint64{"usage_usec": 20}, After: map[string]uint64{"usage_usec": 21}},
	}
	for _, observation := range testCases {
		record := runRecord{Expected: 1, Delivery: deliveryResult{UniqueMeasurement: 1, Unique: 1}, CPU: observation}
		record.evaluate()
		if record.Verdict != verdictInconclusive {
			testContext.Fatalf("record = %+v", record)
		}
	}
}

func TestAssessBacklogIsConservative(testContext *testing.T) {
	stable := make([]seriesPoint, 8)
	for index := range stable {
		stable[index].Outstanding = uint64(index % 2)
	}
	if assessment := assessBacklog(stable); assessment != "not growing" {
		testContext.Fatalf("stable assessment = %q", assessment)
	}
	growing := make([]seriesPoint, 8)
	for index := range growing {
		growing[index].Outstanding = uint64(index * 10)
	}
	if assessment := assessBacklog(growing); assessment != "growing" {
		testContext.Fatalf("growing assessment = %q", assessment)
	}
	if assessment := assessBacklog(make([]seriesPoint, 7)); assessment != "insufficient samples" {
		testContext.Fatalf("short assessment = %q", assessment)
	}
}

func TestVerdictAllowsBoundedTailThatFullyDrains(testContext *testing.T) {
	record := runRecord{
		Expected:               2,
		OutstandingAtWindowEnd: 1,
		OutstandingAfterDrain:  0,
		Delivery:               deliveryResult{UniqueMeasurement: 1, UniqueDrain: 1, Unique: 2},
		CPU:                    CPUObservation{Scope: wholeProcessScope, Before: map[string]uint64{"usage_usec": 10, "nr_throttled": 0}, After: map[string]uint64{"usage_usec": 20, "nr_throttled": 0}},
	}
	record.evaluate()
	if record.Verdict != verdictInconclusive || record.FixtureVerdict != verdictPass {
		testContext.Fatalf("record = %+v", record)
	}
}
