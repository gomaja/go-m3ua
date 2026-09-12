package main

import (
	"bufio"
	"fmt"
	"io"
	"math/bits"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const wholeProcessScope = "whole-process including fixture and library asynchronous work"

type durationPercentiles struct {
	Count uint64        `json:"count"`
	P50   time.Duration `json:"p50_ns"`
	P95   time.Duration `json:"p95_ns"`
	P99   time.Duration `json:"p99_ns"`
	Max   time.Duration `json:"max_ns"`
}

type durationHistogram struct {
	buckets [65]uint64
	count   uint64
	maximum time.Duration
}

func newDurationHistogram() *durationHistogram {
	return &durationHistogram{}
}

func (histogram *durationHistogram) record(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	value := uint64(duration)
	bucket := 0
	if value > 0 {
		bucket = bits.Len64(value)
	}
	histogram.buckets[bucket]++
	histogram.count++
	if duration > histogram.maximum {
		histogram.maximum = duration
	}
}

func (histogram *durationHistogram) percentiles() durationPercentiles {
	return durationPercentiles{
		Count: histogram.count,
		P50:   histogram.quantile(50),
		P95:   histogram.quantile(95),
		P99:   histogram.quantile(99),
		Max:   histogram.maximum,
	}
}

func (histogram *durationHistogram) quantile(percent uint64) time.Duration {
	if histogram.count == 0 {
		return 0
	}
	target := (histogram.count*percent + 99) / 100
	var cumulative uint64
	for bucket, count := range histogram.buckets {
		cumulative += count
		if cumulative < target {
			continue
		}
		if bucket == 0 {
			return 0
		}
		upper := time.Duration(uint64(1) << bucket)
		if upper > histogram.maximum {
			return histogram.maximum
		}
		return upper
	}
	return histogram.maximum
}

func (histogram *durationHistogram) storageBytes() int {
	return len(histogram.buckets) * 8
}

type CPUObservation struct {
	Scope                 string            `json:"scope"`
	Before                map[string]uint64 `json:"before,omitempty"`
	After                 map[string]uint64 `json:"after,omitempty"`
	UsageUsecDelta        uint64            `json:"usage_usec_delta,omitempty"`
	UsageSecondsPerUnique float64           `json:"usage_seconds_per_validated_delivery,omitempty"`
	Error                 string            `json:"error,omitempty"`
}

type runtimeCounters struct {
	Mallocs         uint64 `json:"mallocs"`
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	NumGC           uint32 `json:"num_gc"`
}

type AllocationObservation struct {
	Scope  string          `json:"scope"`
	Before runtimeCounters `json:"before"`
	After  runtimeCounters `json:"after"`
	Delta  runtimeCounters `json:"delta"`
}

func readRuntimeCounters() runtimeCounters {
	var statistics runtime.MemStats
	runtime.ReadMemStats(&statistics)
	return runtimeCounters{
		Mallocs:         statistics.Mallocs,
		TotalAllocBytes: statistics.TotalAlloc,
		HeapAllocBytes:  statistics.HeapAlloc,
		NumGC:           statistics.NumGC,
	}
}

func runtimeDelta(before, after runtimeCounters) runtimeCounters {
	return runtimeCounters{
		Mallocs:         subtractFloor(after.Mallocs, before.Mallocs),
		TotalAllocBytes: subtractFloor(after.TotalAllocBytes, before.TotalAllocBytes),
		HeapAllocBytes:  subtractFloor(after.HeapAllocBytes, before.HeapAllocBytes),
		NumGC:           uint32(subtractFloor(uint64(after.NumGC), uint64(before.NumGC))),
	}
}

func parseCPUStat(reader io.Reader) (map[string]uint64, error) {
	statistics := make(map[string]uint64)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid cpu.stat line %q", scanner.Text())
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse cpu.stat %s: %w", fields[0], err)
		}
		statistics[fields[0]] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, required := range []string{"usage_usec", "nr_throttled"} {
		if _, ok := statistics[required]; !ok {
			return nil, fmt.Errorf("cpu.stat has no %s", required)
		}
	}
	return statistics, nil
}

func readCPUStat(path string) (map[string]uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	return parseCPUStat(file)
}

func subtractFloor(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func newCPUObservation(before, after map[string]uint64, beforeErr, afterErr error, unique uint64) CPUObservation {
	observation := CPUObservation{Scope: wholeProcessScope, Before: before, After: after}
	if beforeErr != nil {
		observation.Error = beforeErr.Error()
	} else if afterErr != nil {
		observation.Error = afterErr.Error()
	} else if !validIncreasingCPUUsage(observation, unique != 0) {
		observation.Error = "cpu.stat usage_usec is missing, unchanged for nonempty work, or decreased"
	} else {
		observation.UsageUsecDelta = after["usage_usec"] - before["usage_usec"]
		if unique != 0 {
			observation.UsageSecondsPerUnique = float64(observation.UsageUsecDelta) / (1e6 * float64(unique))
		}
	}
	return observation
}
