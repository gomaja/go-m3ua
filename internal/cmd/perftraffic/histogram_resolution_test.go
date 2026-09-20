package main

import (
	"testing"
	"time"
)

func TestHistogramSeparatesLatencyBudgetFromOutlier(testContext *testing.T) {
	histogram := newDurationHistogram()
	for index := 0; index < 99; index++ {
		histogram.record(14 * time.Millisecond)
	}
	histogram.record(50 * time.Millisecond)
	if percentile := histogram.percentiles().P99; percentile < 14*time.Millisecond || percentile > 15*time.Millisecond {
		testContext.Fatalf("p99 = %v; want conservative bound between 14ms and 15ms", percentile)
	}
}

func TestHistogramUpperBoundsAcrossDurationRange(testContext *testing.T) {
	for exponent := 0; exponent < 63; exponent++ {
		for _, delta := range []int64{-1, 0, 1} {
			value := (int64(1) << exponent) + delta
			if value < 0 {
				continue
			}
			histogram := newDurationHistogram()
			histogram.record(time.Duration(value))
			histogram.record(time.Duration(1<<63 - 1))
			upper := histogram.percentiles().P50
			if upper < time.Duration(value) || upper-time.Duration(value) > time.Duration(value/64+1) {
				testContext.Fatalf("value %d: upper %v is not within conservative 1/64 resolution", value, upper)
			}
		}
	}
}

func TestHistogramRecordingDoesNotAllocate(testContext *testing.T) {
	histogram := newDurationHistogram()
	allocations := testing.AllocsPerRun(1000, func() { histogram.record(14 * time.Millisecond) })
	if allocations != 0 {
		testContext.Fatalf("allocations = %v", allocations)
	}
}
