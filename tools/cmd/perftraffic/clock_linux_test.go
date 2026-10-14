package main

import "testing"

func TestSharedClockLinuxReadAndDomain(testContext *testing.T) {
	clock, err := newMeasurementClock()
	if err != nil {
		testContext.Fatal(err)
	}
	domain, err := clock.Domain()
	if err != nil || !domain.valid() {
		testContext.Fatalf("Linux clock domain: %+v, %v", domain, err)
	}
	before, err := clock.Now()
	if err != nil {
		testContext.Fatal(err)
	}
	after, err := clock.Now()
	if err != nil || before <= 0 || after < before {
		testContext.Fatalf("Linux clock readings: %d, %d, %v", before, after, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := clock.Now(); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		testContext.Fatalf("clock read allocates %v objects", allocations)
	}
}

func BenchmarkSharedClockLinuxRead(benchmark *testing.B) {
	clock, err := newMeasurementClock()
	if err != nil {
		benchmark.Fatal(err)
	}
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for index := 0; index < benchmark.N; index++ {
		if _, err := clock.Now(); err != nil {
			benchmark.Fatal(err)
		}
	}
}
