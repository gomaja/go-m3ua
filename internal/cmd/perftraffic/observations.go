package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
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
	buckets [128 + 56*64]uint64
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
	bucket := int(value)
	if value >= 128 {
		exponent := bits.Len64(value) - 1
		bucket = 128 + (exponent-7)*64 + int(value>>uint(exponent-6)) - 64
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
	target := histogram.count/100*percent + (histogram.count%100*percent+99)/100
	var cumulative uint64
	for bucket, count := range histogram.buckets {
		cumulative += count
		if cumulative < target {
			continue
		}
		upper := time.Duration(bucket)
		if bucket >= 128 {
			exponent := (bucket-128)/64 + 7
			upper = time.Duration((uint64((bucket-128)%64+65) << uint(exponent-6)) - 1)
		}
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

// Timed-run memory sampling (performance budgets section 4: "During timed runs
// collect low-overhead RSS/runtime counters without forcing collection; record
// peak heap as well as post-GC live heap"). Each second the sampler reads four
// runtime/metrics counters, which unlike runtime.ReadMemStats do not stop the
// world, and the process's resident set from /proc/self/status. It never
// triggers a collection. Nothing here is judged; the observation is recorded
// beside the cohort's verdicts.
const (
	memorySampleInterval = time.Second
	maxMemorySamples     = 601
	memoryScope          = "whole process, sampled once a second from runtime/metrics and /proc/self/status without forcing a collection; " +
		"sampled peaks are lower bounds of the true peaks; heap_live is the heap the previous GC marked live; " +
		"rss_high_water is the kernel's process-lifetime VmHWM at the cohort's start and end, which the kernel maintains lazily from approximate counters and can read below a sampled rss"
)

var memoryMetricNames = [...]string{
	"/memory/classes/heap/objects:bytes",
	"/gc/heap/live:bytes",
	"/gc/heap/goal:bytes",
	"/gc/cycles/total:gc-cycles",
}

// memorySample is one reading. Offset runs from the start of the sampler.
type memorySample struct {
	Offset        time.Duration `json:"offset_ns"`
	HeapBytes     uint64        `json:"heap_bytes"`
	HeapLiveBytes uint64        `json:"heap_live_bytes"`
	HeapGoalBytes uint64        `json:"heap_goal_bytes"`
	GCCycles      uint64        `json:"gc_cycles"`
	RSSBytes      uint64        `json:"rss_bytes,omitempty"`
}

// memoryObservation is one cohort's memory series. HeapPeak is the largest
// sampled heap (live and not yet swept objects), HeapLivePeak and HeapLiveEnd
// the post-GC live heap, GCCycles the collections completed during the
// cohort. RSSPeak is the largest sampled VmRSS. RSSHighWaterStart and
// RSSHighWaterEnd are the kernel's VmHWM, a process-lifetime figure it updates
// lazily (mainly when memory is unmapped) from approximate per-CPU counters:
// it can capture a peak between samples, but it is not a strict upper bound
// on a sampled VmRSS. The RSS fields are absent, with RSSError naming why,
// where the kernel does not provide them; a missing measurement is never a
// zero.
// SamplingTime is the total time the samples took to read, the sampler's own
// cost.
type memoryObservation struct {
	Scope             string         `json:"scope"`
	Interval          time.Duration  `json:"interval_ns"`
	SampleCount       int            `json:"sample_count"`
	Samples           []memorySample `json:"samples"`
	HeapPeak          uint64         `json:"heap_peak_bytes"`
	HeapLivePeak      uint64         `json:"heap_live_peak_bytes"`
	HeapLiveEnd       uint64         `json:"heap_live_end_bytes"`
	HeapGoalPeak      uint64         `json:"heap_goal_peak_bytes"`
	GCCycles          uint64         `json:"gc_cycles"`
	RSSPeak           uint64         `json:"rss_peak_bytes,omitempty"`
	RSSHighWaterStart uint64         `json:"rss_high_water_start_bytes,omitempty"`
	RSSHighWaterEnd   uint64         `json:"rss_high_water_end_bytes,omitempty"`
	RSSError          string         `json:"rss_error,omitempty"`
	MetricsError      string         `json:"metrics_error,omitempty"`
	SamplingTime      time.Duration  `json:"sampling_ns"`
}

// memoryReading is what one read returns.
type memoryReading struct {
	heap, heapLive, heapGoal, gcCycles uint64
	rss, rssHighWater                  uint64
	metricsErr, rssErr                 error
}

// memoryReader owns every buffer a read uses and keeps /proc/self/status open,
// re-reading it from offset zero, so a read allocates nothing.
type memoryReader struct {
	samples []metrics.Sample
	status  *os.File
	openErr error
	buffer  [4096]byte
}

func newMemoryReader() *memoryReader {
	reader := &memoryReader{samples: make([]metrics.Sample, len(memoryMetricNames))}
	for index, name := range memoryMetricNames {
		reader.samples[index].Name = name
	}
	reader.status, reader.openErr = os.Open("/proc/self/status")
	return reader
}

func (reader *memoryReader) read() memoryReading {
	var reading memoryReading
	metrics.Read(reader.samples)
	values := [len(memoryMetricNames)]uint64{}
	for index, sample := range reader.samples {
		if sample.Value.Kind() != metrics.KindUint64 {
			reading.metricsErr = errors.New("runtime metric " + sample.Name + " is not supported")
			continue
		}
		values[index] = sample.Value.Uint64()
	}
	reading.heap, reading.heapLive, reading.heapGoal, reading.gcCycles = values[0], values[1], values[2], values[3]
	if reader.openErr != nil {
		reading.rssErr = reader.openErr
		return reading
	}
	length, err := reader.status.ReadAt(reader.buffer[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		reading.rssErr = err
		return reading
	}
	reading.rss, reading.rssHighWater, reading.rssErr = parseStatusMemory(reader.buffer[:length])
	return reading
}

func (reader *memoryReader) close() {
	if reader.status != nil {
		_ = reader.status.Close()
	}
}

var (
	statusRSSField       = []byte("\nVmRSS:")
	statusHighWaterField = []byte("\nVmHWM:")
)

// parseStatusMemory reads VmRSS and VmHWM, both in kB, from the text of
// /proc/self/status.
func parseStatusMemory(status []byte) (rss, highWater uint64, err error) {
	rss, rssOK := statusKilobytes(status, statusRSSField)
	highWater, highWaterOK := statusKilobytes(status, statusHighWaterField)
	if !rssOK || !highWaterOK {
		return 0, 0, errors.New("/proc/self/status has no well-formed VmRSS and VmHWM")
	}
	return rss, highWater, nil
}

func statusKilobytes(status, field []byte) (uint64, bool) {
	start := bytes.Index(status, field)
	if start < 0 {
		return 0, false
	}
	line := status[start+len(field):]
	if end := bytes.IndexByte(line, '\n'); end >= 0 {
		line = line[:end]
	}
	line = bytes.TrimLeft(line, " \t")
	digits := 0
	var kilobytes uint64
	for digits < len(line) && line[digits] >= '0' && line[digits] <= '9' {
		if kilobytes > (^uint64(0)/1024-9)/10 {
			return 0, false
		}
		kilobytes = kilobytes*10 + uint64(line[digits]-'0')
		digits++
	}
	if digits == 0 || !bytes.Equal(bytes.TrimSpace(line[digits:]), []byte("kB")) {
		return 0, false
	}
	return kilobytes * 1024, true
}

// memorySampler samples on every tick until finish or stop. The first
// reading is taken when it starts and the last when it finishes, so every
// cohort has at least two.
type memorySampler struct {
	read        func() memoryReading
	now         func() time.Time
	release     func()
	start       time.Time
	quit        chan struct{}
	done        chan struct{}
	once        sync.Once
	mutex       sync.Mutex
	finished    bool
	gcStart     uint64
	observation memoryObservation
}

// startMemorySampler samples this process once a second.
func startMemorySampler() *memorySampler {
	reader := newMemoryReader()
	ticker := time.NewTicker(memorySampleInterval)
	return newMemorySampler(reader.read, time.Now, ticker.C, func() {
		ticker.Stop()
		reader.close()
	})
}

func newMemorySampler(read func() memoryReading, now func() time.Time, ticks <-chan time.Time, release func()) *memorySampler {
	sampler := &memorySampler{read: read, now: now, release: release, quit: make(chan struct{}), done: make(chan struct{})}
	sampler.start = now()
	sampler.observation = memoryObservation{Scope: memoryScope, Interval: memorySampleInterval, Samples: []memorySample{}}
	sampler.sample()
	go func() {
		defer close(sampler.done)
		for {
			select {
			case <-ticks:
				sampler.sample()
			case <-sampler.quit:
				return
			}
		}
	}()
	return sampler
}

func (sampler *memorySampler) sample() {
	sampler.mutex.Lock()
	defer sampler.mutex.Unlock()
	began := sampler.now()
	reading := sampler.read()
	ended := sampler.now()
	observation := &sampler.observation
	observation.SamplingTime += max(ended.Sub(began), 0)
	point := memorySample{
		Offset: max(began.Sub(sampler.start), 0), HeapBytes: reading.heap, HeapLiveBytes: reading.heapLive,
		HeapGoalBytes: reading.heapGoal, GCCycles: reading.gcCycles,
	}
	first := observation.SampleCount == 0
	if reading.metricsErr != nil && observation.MetricsError == "" {
		observation.MetricsError = reading.metricsErr.Error()
	}
	if reading.rssErr != nil {
		if observation.RSSError == "" {
			observation.RSSError = reading.rssErr.Error()
		}
	} else if observation.RSSError == "" {
		point.RSSBytes = reading.rss
		observation.RSSPeak = max(observation.RSSPeak, reading.rss)
		if observation.RSSHighWaterStart == 0 {
			observation.RSSHighWaterStart = reading.rssHighWater
		}
		observation.RSSHighWaterEnd = reading.rssHighWater
	}
	if first {
		sampler.gcStart = reading.gcCycles
	}
	observation.GCCycles = subtractFloor(reading.gcCycles, sampler.gcStart)
	observation.HeapPeak = max(observation.HeapPeak, reading.heap)
	observation.HeapLivePeak = max(observation.HeapLivePeak, reading.heapLive)
	observation.HeapLiveEnd = reading.heapLive
	observation.HeapGoalPeak = max(observation.HeapGoalPeak, reading.heapGoal)
	if len(observation.Samples) < maxMemorySamples {
		observation.Samples = append(observation.Samples, point)
	}
	observation.SampleCount++
}

// finish stops sampling, takes the last reading and returns the observation.
// Calling it again returns the same observation.
func (sampler *memorySampler) finish() memoryObservation {
	sampler.halt()
	sampler.mutex.Lock()
	finished := sampler.finished
	sampler.finished = true
	sampler.mutex.Unlock()
	if !finished {
		sampler.sample()
		sampler.release()
	}
	sampler.mutex.Lock()
	defer sampler.mutex.Unlock()
	return sampler.observation
}

// stop ends sampling without a final reading, for a cohort that failed before
// it could finish. It is safe after finish.
func (sampler *memorySampler) stop() {
	sampler.halt()
	sampler.mutex.Lock()
	finished := sampler.finished
	sampler.finished = true
	sampler.mutex.Unlock()
	if !finished {
		sampler.release()
	}
}

func (sampler *memorySampler) halt() {
	sampler.once.Do(func() { close(sampler.quit) })
	<-sampler.done
}
