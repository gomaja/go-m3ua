package main

import (
	"errors"
	"math"
	"time"
)

func scheduledMessages(rate uint64, duration time.Duration) (uint64, error) {
	if rate == 0 || duration <= 0 {
		return 0, errors.New("rate and duration must be positive")
	}
	if uint64(duration) > math.MaxUint64/rate {
		return 0, errors.New("scheduled message count overflows")
	}
	return uint64(duration) * rate / uint64(time.Second), nil
}

func planMessage(cohort string, seed, index uint64, associations int) messageIdentity {
	flow := uint8(index % uint64(flowCount))
	return messageIdentity{
		Cohort:      cohort,
		Seed:        seed,
		Association: uint8(int(flow) % associations),
		Flow:        flow,
		Sequence:    index / uint64(flowCount),
	}
}

func queueCapacities(associations, total int) []int {
	capacities := make([]int, associations)
	for index := range capacities {
		capacities[index] = total / associations
		if index < total%associations {
			capacities[index]++
		}
	}
	return capacities
}
