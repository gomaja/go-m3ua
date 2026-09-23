package main

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

func TestSSNMPlanChunksCoverEveryDestinationAlternating(testContext *testing.T) {
	for _, plan := range []ssnmPlan{{records: 16384, apcs: 1}, {records: 16384, apcs: 1024}, {records: 8, apcs: 2}, {records: 2048, apcs: 1024}, {records: 1000, apcs: 1}} {
		states := make([]int, plan.records)
		position := uint64(0)
		// Three full passes: the preload and two generator passes.
		for updates := 0; updates < 3*plan.records; {
			chunk := plan.chunk(position)
			if chunk.count <= 0 || chunk.first < 0 || chunk.first+chunk.count > plan.records {
				testContext.Fatalf("%+v position %d chunk %+v out of range", plan, position, chunk)
			}
			for destination := chunk.first; destination < chunk.first+chunk.count; destination++ {
				states[destination]++
				want := m3ua.DestinationUnavailable
				if states[destination]%2 == 0 {
					want = m3ua.DestinationAvailable
				}
				if chunk.availability != want {
					testContext.Fatalf("%+v position %d destination %d update %d reports %v, want %v", plan, position, destination, states[destination], chunk.availability, want)
				}
			}
			updates += chunk.count
			position++
			if got := plan.updatesApplied(position); got != uint64(updates) {
				testContext.Fatalf("%+v updatesApplied(%d) = %d, want %d", plan, position, got, updates)
			}
			for destination, count := range states {
				// Large plans check the destinations this position touched and
				// its neighbours; small plans check every destination.
				if plan.records > 2048 && (destination < chunk.first-1 || destination > chunk.first+chunk.count) {
					continue
				}
				want, present := m3ua.DestinationAvailability(0), count > 0
				if present {
					want = m3ua.DestinationAvailable
					if count%2 == 1 {
						want = m3ua.DestinationUnavailable
					}
				}
				got, held := plan.expectedState(destination, position)
				if held != present || got != want {
					testContext.Fatalf("%+v after %d positions destination %d = %v/%v, want %v/%v", plan, position, destination, got, held, want, present)
				}
			}
		}
		for _, count := range states {
			if count != 3 {
				testContext.Fatalf("%+v destination updated %d times over three passes", plan, count)
			}
		}
	}
}

func TestSSNMPlanPreloadAndPeriod(testContext *testing.T) {
	plan := ssnmPlan{records: 16384, apcs: 1}
	if plan.preloadMessages() != 16 || plan.period() != 32768 {
		testContext.Fatalf("preload %d period %d", plan.preloadMessages(), plan.period())
	}
	if chunk := plan.chunk(16); chunk != (ssnmChunk{first: 0, count: 1, availability: m3ua.DestinationAvailable}) {
		testContext.Fatalf("first generator message = %+v", chunk)
	}
	large := ssnmPlan{records: 16384, apcs: 1024}
	if chunk := large.chunk(16 + 16); chunk != (ssnmChunk{first: 0, count: 1024, availability: m3ua.DestinationUnavailable}) {
		testContext.Fatalf("second-pass large message = %+v", chunk)
	}
	odd := ssnmPlan{records: 1000, apcs: 1}
	if odd.preloadMessages() != 1 || odd.chunk(0).count != 1000 {
		testContext.Fatalf("partial preload = %d/%+v", odd.preloadMessages(), odd.chunk(0))
	}
}

func TestSSNMScheduleMatchesDue(testContext *testing.T) {
	for _, rate := range []uint64{1, 3, 7, 10, 1000, 9999, 10000} {
		for message := uint64(0); message < 5000; message++ {
			scheduled := ssnmScheduled(rate, message)
			if got := ssnmDue(rate, scheduled); got != message+1 {
				testContext.Fatalf("rate %d message %d scheduled %d: due %d, want %d", rate, message, scheduled, got, message+1)
			}
			if scheduled > 0 {
				if got := ssnmDue(rate, scheduled-1); got != message {
					testContext.Fatalf("rate %d message %d: due just before schedule %d, want %d", rate, message, got, message)
				}
			}
		}
	}
	if ssnmDue(1000, -1) != 0 || ssnmDue(0, 100) != 0 {
		testContext.Fatal("due before the anchor or at rate zero must be zero")
	}
}

func TestSSNMWindowMessages(testContext *testing.T) {
	anchor := int64(1_000_000_000)
	first, last := ssnmWindowMessages(1000, anchor, anchor+int64(7*time.Second), anchor+int64(37*time.Second))
	if first != 7000 || last != 37000 {
		testContext.Fatalf("window = [%d, %d), want [7000, 37000)", first, last)
	}
	first, last = ssnmWindowMessages(10, anchor, anchor, anchor+int64(30*time.Second))
	if first != 0 || last != 300 {
		testContext.Fatalf("anchored window = [%d, %d), want [0, 300)", first, last)
	}
}
