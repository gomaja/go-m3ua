package main

import (
	"time"

	"github.com/gomaja/go-m3ua"
)

// ssnmPlan is the deterministic SSNM update sequence both processes derive
// from the workload, so the ASP can check every delivered report without any
// side channel.
//
// The sequence is a stream of destination updates u = 0, 1, 2, ... where
// update u names destination u mod records and reports Unavailable on even
// passes (u / records) and Available on odd ones. Positions group updates into
// messages: the first preloadMessages() positions are the preload, 1,024
// updates each, that fills the store with pass 0; every later position m is
// generator message m and carries apcs consecutive updates. Records is a
// multiple of apcs, so one message never straddles two passes and always
// reports one availability.
type ssnmPlan struct {
	records int
	apcs    int
}

type ssnmChunk struct {
	first        int
	count        int
	availability m3ua.DestinationAvailability
}

func (plan ssnmPlan) preloadMessages() uint64 {
	return uint64((plan.records + ssnmPreloadChunk - 1) / ssnmPreloadChunk)
}

// period is how many positions the steady pattern takes to repeat: two
// passes over every destination.
func (plan ssnmPlan) period() uint64 {
	return 2 * uint64(plan.records/plan.apcs)
}

// chunk is the content of one position.
func (plan ssnmPlan) chunk(position uint64) ssnmChunk {
	preload := plan.preloadMessages()
	if position < preload {
		first := int(position) * ssnmPreloadChunk
		return ssnmChunk{first: first, count: min(ssnmPreloadChunk, plan.records-first), availability: m3ua.DestinationUnavailable}
	}
	update := (position - preload) * uint64(plan.apcs)
	pass := 1 + update/uint64(plan.records)
	availability := m3ua.DestinationAvailable
	if pass%2 == 0 {
		availability = m3ua.DestinationUnavailable
	}
	return ssnmChunk{first: int(update % uint64(plan.records)), count: plan.apcs, availability: availability}
}

// updatesApplied is the number of destination updates the first positions
// carry.
func (plan ssnmPlan) updatesApplied(positions uint64) uint64 {
	preload := plan.preloadMessages()
	if positions <= preload {
		return min(positions*ssnmPreloadChunk, uint64(plan.records))
	}
	return uint64(plan.records) + (positions-preload)*uint64(plan.apcs)
}

// expectedState is destination's retained availability once the first
// positions have been applied, and whether any update has named it yet.
func (plan ssnmPlan) expectedState(destination int, positions uint64) (m3ua.DestinationAvailability, bool) {
	updates := plan.updatesApplied(positions)
	count := updates / uint64(plan.records)
	if uint64(destination) < updates%uint64(plan.records) {
		count++
	}
	if count == 0 {
		return 0, false
	}
	if count%2 == 1 {
		return m3ua.DestinationUnavailable, true
	}
	return m3ua.DestinationAvailable, true
}

// ssnmDue reports how many generator messages are scheduled at or before
// elapsed nanoseconds after the anchor. Message m is scheduled at
// floor(m * 1s / rate), so the count is ceil((elapsed+1) * rate / 1s).
func ssnmDue(rate uint64, elapsed int64) uint64 {
	if rate == 0 || elapsed < 0 {
		return 0
	}
	return ((uint64(elapsed)+1)*rate-1)/uint64(time.Second) + 1
}

// ssnmScheduled is generator message m's scheduled offset from the anchor.
func ssnmScheduled(rate, message uint64) int64 {
	return int64(message * uint64(time.Second) / rate)
}

// ssnmWindowMessages is the half-open range of generator messages scheduled
// inside the shared window [start, end).
func ssnmWindowMessages(rate uint64, anchor, start, end int64) (uint64, uint64) {
	return ssnmDue(rate, start-anchor-1), ssnmDue(rate, end-anchor-1)
}

// ssnmDestinations lists every generated destination once, in order; each
// message names a contiguous slice of it.
func ssnmDestinations(records int) []m3ua.PointCodeRange {
	destinations := make([]m3ua.PointCodeRange, records)
	for index := range destinations {
		destinations[index] = m3ua.PointCodeRange{PointCode: ssnmPointCodeBase + uint32(index)}
	}
	return destinations
}
