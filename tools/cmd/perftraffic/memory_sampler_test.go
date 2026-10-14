package main

import (
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"
)

// steppedClock returns start, start+step, start+2*step, ... on successive
// calls, so offsets and sampling time are exact without a wall clock.
func steppedClock(start time.Time, step time.Duration) func() time.Time {
	calls := 0
	return func() time.Time {
		instant := start.Add(time.Duration(calls) * step)
		calls++
		return instant
	}
}

// scriptedReadings returns the readings in order, then repeats the last.
func scriptedReadings(readings ...memoryReading) func() memoryReading {
	index := 0
	return func() memoryReading {
		reading := readings[min(index, len(readings)-1)]
		index++
		return reading
	}
}

// The sampler reads on start, on every tick and on finish. Peaks are the
// largest readings, the live heap ends at the last reading, collections are
// counted from the first reading, and the RSS high-water mark is kept at
// both ends so a rise during the cohort is visible.
func TestMemorySamplerRecordsPeaksAndTheLiveHeap(testContext *testing.T) {
	readings := []memoryReading{
		{heap: 10 << 20, heapLive: 4 << 20, heapGoal: 8 << 20, gcCycles: 5, rss: 30 << 20, rssHighWater: 40 << 20},
		{heap: 50 << 20, heapLive: 6 << 20, heapGoal: 12 << 20, gcCycles: 7, rss: 45 << 20, rssHighWater: 45 << 20},
		{heap: 20 << 20, heapLive: 9 << 20, heapGoal: 18 << 20, gcCycles: 9, rss: 44 << 20, rssHighWater: 46 << 20},
		{heap: 15 << 20, heapLive: 5 << 20, heapGoal: 10 << 20, gcCycles: 12, rss: 41 << 20, rssHighWater: 46 << 20},
	}
	ticks := make(chan time.Time)
	released := 0
	start := time.Unix(1_000, 0)
	sampler := newMemorySampler(scriptedReadings(readings...), steppedClock(start, 250*time.Millisecond), ticks, func() { released++ })
	ticks <- time.Time{}
	ticks <- time.Time{}
	observation := sampler.finish()
	if again := sampler.finish(); again.SampleCount != observation.SampleCount {
		testContext.Fatalf("second finish changed the observation: %+v", again)
	}
	sampler.stop()
	if released != 1 {
		testContext.Fatalf("resources released %d times, want once", released)
	}
	if observation.SampleCount != 4 || len(observation.Samples) != 4 || observation.Scope != memoryScope || observation.Interval != time.Second {
		testContext.Fatalf("observation = %+v", observation)
	}
	if observation.HeapPeak != 50<<20 || observation.HeapLivePeak != 9<<20 || observation.HeapLiveEnd != 5<<20 ||
		observation.HeapGoalPeak != 18<<20 || observation.GCCycles != 7 {
		testContext.Fatalf("heap = peak %d live peak %d live end %d goal peak %d cycles %d", observation.HeapPeak,
			observation.HeapLivePeak, observation.HeapLiveEnd, observation.HeapGoalPeak, observation.GCCycles)
	}
	if observation.RSSPeak != 45<<20 || observation.RSSHighWaterStart != 40<<20 || observation.RSSHighWaterEnd != 46<<20 || observation.RSSError != "" {
		testContext.Fatalf("rss = peak %d high water %d..%d error %q", observation.RSSPeak, observation.RSSHighWaterStart,
			observation.RSSHighWaterEnd, observation.RSSError)
	}
	// The clock is read once at construction and twice per sample, 250 ms
	// apart: each read takes 250 ms and sample i begins 250 ms + i*500 ms in.
	for index, sample := range observation.Samples {
		if sample.Offset != 250*time.Millisecond+time.Duration(index)*500*time.Millisecond || sample.HeapBytes != readings[index].heap ||
			sample.RSSBytes != readings[index].rss || sample.GCCycles != readings[index].gcCycles {
			testContext.Fatalf("sample %d = %+v", index, sample)
		}
	}
	if observation.SamplingTime != 4*250*time.Millisecond {
		testContext.Fatalf("sampling time = %s", observation.SamplingTime)
	}
}

// Where the kernel provides no resident-set figures the observation names the
// failure and records no RSS value: a missing measurement is never a zero.
func TestMemorySamplerNeverReportsAMissingRSSAsZero(testContext *testing.T) {
	failure := errors.New("open /proc/self/status: no such file or directory")
	sampler := newMemorySampler(scriptedReadings(memoryReading{heap: 1 << 20, heapGoal: 4 << 20, rssErr: failure}),
		steppedClock(time.Unix(0, 0), time.Millisecond), make(chan time.Time), func() {})
	observation := sampler.finish()
	if observation.RSSError != failure.Error() || observation.RSSPeak != 0 || observation.RSSHighWaterEnd != 0 || observation.HeapPeak != 1<<20 {
		testContext.Fatalf("observation = %+v", observation)
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		testContext.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		testContext.Fatal(err)
	}
	for _, name := range []string{"rss_peak_bytes", "rss_high_water_start_bytes", "rss_high_water_end_bytes"} {
		if _, present := fields[name]; present {
			testContext.Fatalf("%s serialized without a measurement: %s", name, encoded)
		}
	}
	if samples := fields["samples"].([]any); len(samples) != 2 || samples[0].(map[string]any)["rss_bytes"] != nil {
		testContext.Fatalf("samples = %v", samples)
	}
}

// The retained series is bounded; the peaks still cover every reading.
func TestMemorySamplerBoundsTheRetainedSeries(testContext *testing.T) {
	heap := uint64(0)
	read := func() memoryReading {
		heap++
		return memoryReading{heap: heap}
	}
	ticks := make(chan time.Time)
	sampler := newMemorySampler(read, steppedClock(time.Unix(0, 0), time.Millisecond), ticks, func() {})
	for range maxMemorySamples + 10 {
		ticks <- time.Time{}
	}
	observation := sampler.finish()
	if len(observation.Samples) != maxMemorySamples || observation.SampleCount != maxMemorySamples+12 || observation.HeapPeak != uint64(maxMemorySamples+12) {
		testContext.Fatalf("retained %d of %d samples, heap peak %d", len(observation.Samples), observation.SampleCount, observation.HeapPeak)
	}
}

// The real reader reads this process without allocating, so sampling adds no
// garbage and cannot provoke a collection, and it reports plausible figures:
// a nonzero heap and heap goal and, on Linux, a nonzero resident set and
// high-water mark. VmHWM is maintained lazily from approximate counters, so it
// is not compared with VmRSS.
func TestMemoryReaderReadsThisProcessWithoutAllocating(testContext *testing.T) {
	reader := newMemoryReader()
	defer reader.close()
	reading := reader.read()
	if reading.metricsErr != nil || reading.heap == 0 || reading.heapGoal == 0 {
		testContext.Fatalf("runtime reading = %+v", reading)
	}
	if runtime.GOOS == "linux" {
		if reading.rssErr != nil || reading.rss == 0 || reading.rssHighWater == 0 {
			testContext.Fatalf("Linux resident set = %+v", reading)
		}
	} else if reading.rssErr == nil {
		testContext.Fatalf("resident set reported without /proc: %+v", reading)
	}
	if allocations := testing.AllocsPerRun(100, func() { _ = reader.read() }); allocations != 0 {
		testContext.Fatalf("a memory reading allocated %.1f times", allocations)
	}
}

func TestParseStatusMemory(testContext *testing.T) {
	status := "Name:\tperftraffic\nVmPeak:\t 1300 kB\nVmHWM:\t   20480 kB\nVmRSS:\t   10240 kB\nThreads:\t9\n"
	if rss, highWater, err := parseStatusMemory([]byte(status)); err != nil || rss != 10240*1024 || highWater != 20480*1024 {
		testContext.Fatalf("parse = %d %d %v", rss, highWater, err)
	}
	for name, text := range map[string]string{
		"missing rss":   "Name:\tx\nVmHWM:\t 1 kB\n",
		"missing hwm":   "Name:\tx\nVmRSS:\t 1 kB\n",
		"wrong unit":    "Name:\tx\nVmHWM:\t 1 kB\nVmRSS:\t 1 MB\n",
		"no digits":     "Name:\tx\nVmHWM:\t 1 kB\nVmRSS:\t kB\n",
		"overflow":      "Name:\tx\nVmHWM:\t 1 kB\nVmRSS:\t 18446744073709551615 kB\n",
		"trailing text": "Name:\tx\nVmHWM:\t 1 kB\nVmRSS:\t 12x kB\n",
	} {
		if _, _, err := parseStatusMemory([]byte(text)); err == nil {
			testContext.Errorf("%s: malformed status accepted", name)
		}
	}
}

// A timed receiver cohort records its memory from start to stop.
func TestReceiverCohortRecordsMemory(testContext *testing.T) {
	control, _, _ := sharedClockFixture(testContext)
	if control.result().Memory != nil {
		testContext.Fatal("memory recorded before the cohort stopped")
	}
	_ = control.stop()
	memory := control.result().Memory
	if memory == nil || memory.SampleCount < 2 || memory.HeapPeak == 0 || memory.HeapGoalPeak == 0 || memory.MetricsError != "" {
		testContext.Fatalf("receiver memory = %+v", memory)
	}
	if runtime.GOOS == "linux" && (memory.RSSPeak == 0 || memory.RSSHighWaterEnd == 0 || memory.RSSError != "") {
		testContext.Fatalf("receiver resident set = %+v", memory)
	}
}

// BenchmarkMemoryReading measures one timed-run memory reading, the whole
// per-second cost of the sampler: at one reading a second its CPU share is
// the reported ns/op divided by 1e9.
func BenchmarkMemoryReading(benchmark *testing.B) {
	reader := newMemoryReader()
	defer reader.close()
	benchmark.ReportAllocs()
	for range benchmark.N {
		_ = reader.read()
	}
}
