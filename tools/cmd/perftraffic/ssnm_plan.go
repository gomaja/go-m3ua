package main

import (
	"time"

	"github.com/gomaja/go-m3ua"
)

// ssnmPlan is the deterministic SSNM update sequence both processes derive
// from the workload, so the ASP can check every delivered report without any
// side channel.
//
// Every association is its own ASP partition with its own records
// destinations: association a names point codes ssnmPointCodeBase +
// a*records + d for d in [0, records), so the destinations of two partitions
// never overlap and a report's content names the partition it belongs to.
//
// Within one partition the sequence is a stream of destination updates u = 0,
// 1, 2, ... where update u names destination u mod records and reports
// Unavailable on even passes (u / records) and Available on odd ones.
// Positions group updates into messages: the first preloadMessages()
// positions are the preload, 1,024 updates each, that fills the partition
// with pass 0; every later position carries apcs consecutive updates. Records
// is a multiple of apcs, so one message never straddles two passes and always
// reports one availability.
//
// The generator schedules one global message sequence at the total rate and
// sends message m to association m mod associations only, as that
// partition's position preloadMessages() + m / associations (target). Each
// partition therefore receives every associations-th message, at its own
// scheduled instant, and all partitions follow the same per-partition plan.
type ssnmPlan struct {
	records      int
	apcs         int
	associations int
}

type ssnmChunk struct {
	first        int
	count        int
	availability m3ua.DestinationAvailability
}

func newSSNMPlan(config ssnmConfig, associations int) ssnmPlan {
	return ssnmPlan{records: config.Records, apcs: config.APCs, associations: associations}
}

func (plan ssnmPlan) preloadMessages() uint64 {
	return uint64((plan.records + ssnmPreloadChunk - 1) / ssnmPreloadChunk)
}

// preloadSteps is the number of preload messages over every partition. The
// ASP requests them one at a time (preloadStep) and waits until every
// subscriber has consumed each before asking for the next.
func (plan ssnmPlan) preloadSteps() uint64 {
	return uint64(plan.associations) * plan.preloadMessages()
}

// preloadStep maps preload step s to its association and position:
// association-major, so one partition is filled before the next.
func (plan ssnmPlan) preloadStep(step uint64) (int, uint64) {
	preload := plan.preloadMessages()
	return int(step / preload), step % preload
}

// period is how many positions the steady pattern of one partition takes to
// repeat: two passes over every destination.
func (plan ssnmPlan) period() uint64 {
	return 2 * uint64(plan.records/plan.apcs)
}

// chunk is the content of one position, in partition-local destinations.
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

// target is the association generator message m is sent to and the position
// it takes in that association's partition.
func (plan ssnmPlan) target(message uint64) (int, uint64) {
	associations := uint64(plan.associations)
	return int(message % associations), plan.preloadMessages() + message/associations
}

// message is the generator message a partition position carries, and false
// for a preload position.
func (plan ssnmPlan) message(association int, position uint64) (uint64, bool) {
	preload := plan.preloadMessages()
	if position < preload {
		return 0, false
	}
	return (position-preload)*uint64(plan.associations) + uint64(association), true
}

// messagesFor counts the generator messages among the first messages that go
// to association.
func (plan ssnmPlan) messagesFor(association int, messages uint64) uint64 {
	associations := uint64(plan.associations)
	if messages <= uint64(association) {
		return 0
	}
	return (messages-uint64(association)-1)/associations + 1
}

// expectedPositions is every partition's next position once the preload and
// the first messages generator messages have been delivered, by association.
func (plan ssnmPlan) expectedPositions(messages uint64) []uint64 {
	positions := make([]uint64, plan.associations)
	for association := range positions {
		positions[association] = plan.preloadMessages() + plan.messagesFor(association, messages)
	}
	return positions
}

// pointCode is the point code of a partition-local destination.
func (plan ssnmPlan) pointCode(association, destination int) uint32 {
	return ssnmPointCodeBase + uint32(association*plan.records+destination)
}

// locate maps a point code back to its association and partition-local
// destination.
func (plan ssnmPlan) locate(pointCode uint32) (int, int, bool) {
	offset := int64(pointCode) - int64(ssnmPointCodeBase)
	if offset < 0 || offset >= int64(plan.records)*int64(plan.associations) {
		return 0, 0, false
	}
	return int(offset / int64(plan.records)), int(offset % int64(plan.records)), true
}

// updatesApplied is the number of destination updates the first positions of
// one partition carry.
func (plan ssnmPlan) updatesApplied(positions uint64) uint64 {
	preload := plan.preloadMessages()
	if positions <= preload {
		return min(positions*ssnmPreloadChunk, uint64(plan.records))
	}
	return uint64(plan.records) + (positions-preload)*uint64(plan.apcs)
}

// expectedState is a partition-local destination's retained availability
// once the first positions of its partition have been applied, and whether
// any update has named it yet.
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
// floor(m * 1s / rate), so the count is ceil((elapsed+1) * rate / 1s). The
// rate is the total over every association.
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
