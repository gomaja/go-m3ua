package main

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

func TestSSNMPlanChunksCoverEveryDestinationAlternating(testContext *testing.T) {
	for _, plan := range []ssnmPlan{{records: 16384, apcs: 1, associations: 1}, {records: 16384, apcs: 1024, associations: 1}, {records: 8, apcs: 2, associations: 1}, {records: 2048, apcs: 1024, associations: 8}, {records: 1000, apcs: 1, associations: 1}} {
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
	plan := ssnmPlan{records: 16384, apcs: 1, associations: 1}
	if plan.preloadMessages() != 16 || plan.period() != 32768 {
		testContext.Fatalf("preload %d period %d", plan.preloadMessages(), plan.period())
	}
	if chunk := plan.chunk(16); chunk != (ssnmChunk{first: 0, count: 1, availability: m3ua.DestinationAvailable}) {
		testContext.Fatalf("first generator message = %+v", chunk)
	}
	large := ssnmPlan{records: 16384, apcs: 1024, associations: 1}
	if chunk := large.chunk(16 + 16); chunk != (ssnmChunk{first: 0, count: 1024, availability: m3ua.DestinationUnavailable}) {
		testContext.Fatalf("second-pass large message = %+v", chunk)
	}
	odd := ssnmPlan{records: 1000, apcs: 1, associations: 1}
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

// ssnmSchedulePoint is one generator message as the schedule places it: the
// association it goes to, the position it takes in that association's
// partition and its offset from the anchor.
type ssnmSchedulePoint struct {
	message     uint64
	association int
	position    uint64
	offset      time.Duration
}

// The total rate is spread round-robin: message m goes to association m mod
// N, each at its own instant floor(m * 1s / rate), so 1,000/s is one message
// per millisecond and 10/s one every 100 ms whatever N is. Every value below
// is worked by hand.
func TestSSNMRoundRobinAssignment(testContext *testing.T) {
	// 2,048 records per partition: two preload positions per association.
	for _, testCase := range []struct {
		name         string
		rate         uint64
		associations int
		points       []ssnmSchedulePoint
	}{
		{"one association at 1,000/s", 1000, 1, []ssnmSchedulePoint{
			{0, 0, 2, 0}, {1, 0, 3, time.Millisecond}, {7, 0, 9, 7 * time.Millisecond}, {8, 0, 10, 8 * time.Millisecond}, {999, 0, 1001, 999 * time.Millisecond}, {1000, 0, 1002, time.Second},
		}},
		{"one association at 10/s", 10, 1, []ssnmSchedulePoint{
			{0, 0, 2, 0}, {1, 0, 3, 100 * time.Millisecond}, {9, 0, 11, 900 * time.Millisecond}, {10, 0, 12, time.Second}, {1199, 0, 1201, 119900 * time.Millisecond},
		}},
		{"eight associations at 1,000/s", 1000, 8, []ssnmSchedulePoint{
			{0, 0, 2, 0}, {1, 1, 2, time.Millisecond}, {2, 2, 2, 2 * time.Millisecond}, {7, 7, 2, 7 * time.Millisecond},
			{8, 0, 3, 8 * time.Millisecond}, {9, 1, 3, 9 * time.Millisecond}, {15, 7, 3, 15 * time.Millisecond}, {16, 0, 4, 16 * time.Millisecond},
			{999, 7, 126, 999 * time.Millisecond}, {1000, 0, 127, time.Second},
		}},
		{"eight associations at 10/s", 10, 8, []ssnmSchedulePoint{
			{0, 0, 2, 0}, {1, 1, 2, 100 * time.Millisecond}, {7, 7, 2, 700 * time.Millisecond},
			{8, 0, 3, 800 * time.Millisecond}, {9, 1, 3, 900 * time.Millisecond}, {10, 2, 3, time.Second}, {15, 7, 3, 1500 * time.Millisecond},
			{16, 0, 4, 1600 * time.Millisecond}, {1199, 7, 151, 119900 * time.Millisecond},
		}},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			plan := ssnmPlan{records: 2048, apcs: 1, associations: testCase.associations}
			for _, point := range testCase.points {
				association, position := plan.target(point.message)
				offset := time.Duration(ssnmScheduled(testCase.rate, point.message))
				if association != point.association || position != point.position || offset != point.offset {
					testContext.Fatalf("message %d goes to association %d position %d at %s, want association %d position %d at %s",
						point.message, association, position, offset, point.association, point.position, point.offset)
				}
				if message, generated := plan.message(association, position); !generated || message != point.message {
					testContext.Fatalf("association %d position %d maps back to message %d (%t), want %d", association, position, message, generated, point.message)
				}
			}
			// Consecutive messages of one association are N intervals apart:
			// 8 ms and 800 ms with eight associations, 1 ms and 100 ms with one.
			interval := time.Duration(testCase.associations) * time.Second / time.Duration(testCase.rate)
			for message := uint64(0); message < 64; message++ {
				gap := time.Duration(ssnmScheduled(testCase.rate, message+uint64(testCase.associations)) - ssnmScheduled(testCase.rate, message))
				if gap != interval {
					testContext.Fatalf("association %d waits %s between messages %d and %d, want %s", message%uint64(testCase.associations), gap, message, message+uint64(testCase.associations), interval)
				}
			}
		})
	}
}

// Over any window the schedule offers rate x duration messages in total, and
// round-robin splits them evenly: a 120-second window at 1,000/s holds
// 120,000 messages, 15,000 per association with eight; at 10/s, 1,200 and
// 150. The window may start anywhere relative to the anchor.
func TestSSNMWindowTotalEqualsRateTimesDuration(testContext *testing.T) {
	anchor := int64(5_000_000_000)
	for _, testCase := range []struct {
		rate         uint64
		associations int
		duration     time.Duration
	}{
		{1000, 1, 120 * time.Second}, {1000, 8, 120 * time.Second}, {10, 1, 120 * time.Second}, {10, 8, 120 * time.Second},
		{1000, 8, 3 * time.Second}, {10, 8, 2 * time.Second},
	} {
		for _, start := range []time.Duration{0, 7 * time.Second, 7*time.Second + 500*time.Microsecond, 7*time.Second + 50*time.Millisecond, 30*time.Second + 1} {
			first, last := ssnmWindowMessages(testCase.rate, anchor, anchor+int64(start), anchor+int64(start+testCase.duration))
			want := testCase.rate * uint64(testCase.duration/time.Millisecond) / 1000
			if last-first != want {
				testContext.Fatalf("%d/s over %s from %s: %d messages, want %d", testCase.rate, testCase.duration, start, last-first, want)
			}
			plan := ssnmPlan{records: 2048, apcs: 1, associations: testCase.associations}
			var total uint64
			for association := 0; association < testCase.associations; association++ {
				share := plan.messagesFor(association, last) - plan.messagesFor(association, first)
				if share*uint64(testCase.associations) != want && want%uint64(testCase.associations) == 0 {
					testContext.Fatalf("%d/s over %s from %s: association %d gets %d of %d", testCase.rate, testCase.duration, start, association, share, want)
				}
				total += share
			}
			if total != want {
				testContext.Fatalf("%d/s over %s from %s: shares add up to %d, want %d", testCase.rate, testCase.duration, start, total, want)
			}
		}
	}
}

// messagesFor and expectedPositions agree with a brute-force count, and every
// partition's destinations are its own.
func TestSSNMPlanPartitionsAreDisjoint(testContext *testing.T) {
	plan := ssnmPlan{records: 2048, apcs: 1024, associations: 8}
	for messages := uint64(0); messages < 40; messages++ {
		counts := make([]uint64, plan.associations)
		for message := uint64(0); message < messages; message++ {
			counts[message%8]++
		}
		expected := plan.expectedPositions(messages)
		for association, count := range counts {
			if plan.messagesFor(association, messages) != count || expected[association] != plan.preloadMessages()+count {
				testContext.Fatalf("after %d messages association %d: messagesFor %d expected %d, want %d and %d",
					messages, association, plan.messagesFor(association, messages), expected[association], count, plan.preloadMessages()+count)
			}
		}
	}
	if plan.preloadSteps() != 16 {
		testContext.Fatalf("preload steps = %d, want 8 associations x 2", plan.preloadSteps())
	}
	for step, want := range map[uint64][2]uint64{0: {0, 0}, 1: {0, 1}, 2: {1, 0}, 15: {7, 1}} {
		association, position := plan.preloadStep(step)
		if uint64(association) != want[0] || position != want[1] {
			testContext.Fatalf("preload step %d = association %d position %d, want %v", step, association, position, want)
		}
	}
	seen := make(map[uint32]bool)
	for association := 0; association < plan.associations; association++ {
		for destination := 0; destination < plan.records; destination++ {
			pointCode := plan.pointCode(association, destination)
			owner, local, found := plan.locate(pointCode)
			if seen[pointCode] || !found || owner != association || local != destination {
				testContext.Fatalf("association %d destination %d = %#x located at %d/%d (%t)", association, destination, pointCode, owner, local, found)
			}
			seen[pointCode] = true
		}
	}
	for _, pointCode := range []uint32{ssnmPointCodeBase - 1, ssnmPointCodeBase + 8*2048, 0x220000} {
		if _, _, found := plan.locate(pointCode); found {
			testContext.Fatalf("point code %#x outside every partition located", pointCode)
		}
	}
	if last := (ssnmPlan{records: ssnmMaxRecords, apcs: 1, associations: maxAssociations}).pointCode(maxAssociations-1, ssnmMaxRecords-1); last != 0x47ffff {
		testContext.Fatalf("largest generated point code = %#x, want 0x47ffff", last)
	}
}
