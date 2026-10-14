package main

import (
	"testing"
	"time"
)

func TestScheduledMessagesUsesExactFloorWithoutOverflow(testContext *testing.T) {
	testCases := []struct {
		rate     uint64
		duration time.Duration
		want     uint64
	}{
		{rate: 25_000, duration: time.Second, want: 25_000},
		{rate: 3, duration: 1500 * time.Millisecond, want: 4},
		{rate: 1_000_000, duration: 10 * time.Minute, want: 600_000_000},
	}
	for _, testCase := range testCases {
		got, err := scheduledMessages(testCase.rate, testCase.duration)
		if err != nil {
			testContext.Fatalf("scheduledMessages(%d, %s): %v", testCase.rate, testCase.duration, err)
		}
		if got != testCase.want {
			testContext.Fatalf("scheduledMessages(%d, %s) = %d, want %d", testCase.rate, testCase.duration, got, testCase.want)
		}
	}
}

func TestPlanMessageUsesStableFlowToAssociationAssignment(testContext *testing.T) {
	for index := uint64(0); index < 10_000; index++ {
		identity := planMessage("cohort", 7, index, 8)
		if int(identity.Association) != int(identity.Flow)%8 {
			testContext.Fatalf("index %d identity = %+v", index, identity)
		}
		if identity.Sequence*uint64(flowCount)+uint64(identity.Flow) != index {
			testContext.Fatalf("index %d identity does not round-trip: %+v", index, identity)
		}
	}
}

func TestQueueCapacitiesSumToOutstandingLimit(testContext *testing.T) {
	for associations := 1; associations <= maxAssociations; associations++ {
		capacities := queueCapacities(associations, maxOutstanding)
		total := 0
		for _, capacity := range capacities {
			total += capacity
		}
		if total != maxOutstanding {
			testContext.Fatalf("associations %d total capacity = %d, want %d", associations, total, maxOutstanding)
		}
	}
}
