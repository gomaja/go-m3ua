package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"net/http"
	"strconv"
	"time"

	"github.com/gomaja/go-m3ua"
)

// overloadQueuePollInterval is how often an overload receiver observes each
// association's inbound DATA queue. The library bounds the queue itself; the
// poll only records the occupancy it reaches, so a finer interval catches
// higher maxima at a negligible cost (a length, a capacity and two atomic
// loads per association).
const overloadQueuePollInterval = 10 * time.Millisecond

const overloadReceiverScope = "receiver-side overload observations: validated unique deliveries by scheduled phase, the per-message delivered ledger, and each association's inbound DATA queue as Association.DataQueueStats reports it"

// bitmap is a fixed-size set of message indexes.
type bitmap []uint64

func newBitmap(size uint64) bitmap {
	return make(bitmap, (size+63)/64)
}

func (set bitmap) add(index uint64) {
	set[index/64] |= 1 << (index % 64)
}

func (set bitmap) has(index uint64) bool {
	return set[index/64]&(1<<(index%64)) != 0
}

func (set bitmap) count() uint64 {
	var total uint64
	for _, word := range set {
		total += uint64(bits.OnesCount64(word))
	}
	return total
}

// countAndNot counts members of set that are not members of other.
func (set bitmap) countAndNot(other bitmap) uint64 {
	var total uint64
	for index, word := range set {
		if index < len(other) {
			word &^= other[index]
		}
		total += uint64(bits.OnesCount64(word))
	}
	return total
}

// countAnd counts members common to set and other.
func (set bitmap) countAnd(other bitmap) uint64 {
	var total uint64
	for index, word := range set {
		if index < len(other) {
			total += uint64(bits.OnesCount64(word & other[index]))
		}
	}
	return total
}

func (set bitmap) union(other bitmap) bitmap {
	result := append(bitmap(nil), set...)
	for index := range result {
		if index < len(other) {
			result[index] |= other[index]
		}
	}
	return result
}

func (set bitmap) encode() []byte {
	encoded := make([]byte, len(set)*8)
	for index, word := range set {
		binary.LittleEndian.PutUint64(encoded[index*8:], word)
	}
	return encoded
}

func decodeBitmap(encoded []byte, size uint64) (bitmap, error) {
	words := (size + 63) / 64
	if uint64(len(encoded)) != words*8 {
		return nil, fmt.Errorf("delivered ledger is %d bytes, want %d", len(encoded), words*8)
	}
	set := make(bitmap, words)
	for index := range set {
		set[index] = binary.LittleEndian.Uint64(encoded[index*8:])
	}
	if size%64 != 0 && set[len(set)-1]>>(size%64) != 0 {
		return nil, errors.New("delivered ledger names messages outside the schedule")
	}
	return set, nil
}

type overloadAssociationObservation struct {
	Index              int    `json:"index"`
	QueueCapacity      int    `json:"queue_capacity"`
	MaxQueued          int    `json:"max_queued"`
	Discarded          uint64 `json:"discarded"`
	CongestionEpisodes uint64 `json:"congestion_episodes"`
	CongestedAtEnd     bool   `json:"congested_at_end"`
	State              string `json:"state"`
	Active             bool   `json:"active"`
	EpochStart         uint64 `json:"epoch_start"`
	EpochEnd           uint64 `json:"epoch_end"`
}

type overloadReceiverPoint struct {
	OffsetMillis uint64 `json:"offset_ms"`
	Unique       uint64 `json:"unique"`
	Discarded    uint64 `json:"discarded"`
	Queued       uint64 `json:"queued"`
	MaxQueued    int    `json:"max_queued"`
}

type overloadLedgerSummary struct {
	Messages  uint64 `json:"messages"`
	Delivered uint64 `json:"delivered"`
}

// overloadReceiverRecord is the receiver's overload object.
type overloadReceiverRecord struct {
	Scope                   string                           `json:"scope"`
	ConfiguredQueueCapacity int                              `json:"configured_queue_capacity"`
	QueuePollInterval       time.Duration                    `json:"queue_poll_interval_ns"`
	QueuePolls              uint64                           `json:"queue_polls"`
	UniqueByPhase           []uint64                         `json:"unique_by_phase"`
	DuplicateByPhase        []uint64                         `json:"duplicate_by_phase"`
	Misscoped               uint64                           `json:"misscoped"`
	Discarded               uint64                           `json:"discarded"`
	Associations            []overloadAssociationObservation `json:"associations"`
	DeliveredLedger         overloadLedgerSummary            `json:"delivered_ledger"`
	PeakRSSBytes            uint64                           `json:"peak_rss_bytes,omitempty"`
	PeakRSSError            string                           `json:"peak_rss_error,omitempty"`
	Series                  []overloadReceiverPoint          `json:"series"`
}

// receiverOverloadProgress is what each /progress snapshot adds for an
// overload cohort. The discard total is read immediately before and after the
// unique count, so the discards at the instant unique was captured lie between
// the two.
type receiverOverloadProgress struct {
	DiscardedBefore uint64 `json:"discarded_before"`
	DiscardedAfter  uint64 `json:"discarded_after"`
	Queued          uint64 `json:"queued"`
}

// receiverOverloadState is the receiver's per-cohort overload accounting. It is
// guarded by the control mutex.
type receiverOverloadState struct {
	schedule         *phasedSchedule
	uniqueByPhase    []uint64
	duplicateByPhase []uint64
	misscoped        uint64
	delivered        bitmap
	associations     []*m3ua.Association
	discardBase      []uint64
	epochStart       []uint64
	maxQueued        []int
	capacity         []int
	episodes         []uint64
	congested        []bool
	polls            uint64
	final            []overloadAssociationObservation
	series           []overloadReceiverPoint
	stopPoll         chan struct{}
	// begun is set once the cohort starts measuring and the association
	// baselines exist.
	begun bool
}

func newReceiverOverloadState(schedule *phasedSchedule) *receiverOverloadState {
	return &receiverOverloadState{
		schedule:         schedule,
		uniqueByPhase:    make([]uint64, len(schedule.phases)),
		duplicateByPhase: make([]uint64, len(schedule.phases)),
		delivered:        newBitmap(schedule.expected),
	}
}

// begin snapshots every association's cumulative discard count and epoch at
// the start of the cohort, so the record reports this cohort's discards only.
func (state *receiverOverloadState) begin(associations []*m3ua.Association) {
	state.begun = true
	state.associations = append([]*m3ua.Association(nil), associations...)
	count := len(state.associations)
	state.discardBase = make([]uint64, count)
	state.epochStart = make([]uint64, count)
	state.maxQueued = make([]int, count)
	state.capacity = make([]int, count)
	state.episodes = make([]uint64, count)
	state.congested = make([]bool, count)
	for index, association := range state.associations {
		stats := association.DataQueueStats()
		state.discardBase[index] = stats.Discarded
		state.epochStart[index] = association.Epoch()
		state.capacity[index] = stats.Capacity
		state.maxQueued[index] = stats.Queued
		state.congested[index] = stats.Congested
	}
}

// observeQueues takes one occupancy observation of every inbound queue and
// returns the total discards this cohort and the total queued.
func (state *receiverOverloadState) observeQueues() (discarded, queued uint64) {
	for index, association := range state.associations {
		stats := association.DataQueueStats()
		state.maxQueued[index] = max(state.maxQueued[index], stats.Queued)
		state.capacity[index] = stats.Capacity
		if stats.Congested && !state.congested[index] {
			state.episodes[index]++
		}
		state.congested[index] = stats.Congested
		discarded += subtractFloor(stats.Discarded, state.discardBase[index])
		queued += uint64(stats.Queued)
	}
	state.polls++
	return discarded, queued
}

// discarded reads the current cohort discard total without recording an
// occupancy observation.
func (state *receiverOverloadState) discarded() uint64 {
	var total uint64
	for index, association := range state.associations {
		total += subtractFloor(association.DataQueueStats().Discarded, state.discardBase[index])
	}
	return total
}

func (state *receiverOverloadState) recordUnique(index uint64) {
	if index >= state.schedule.expected {
		return
	}
	state.delivered.add(index)
	state.uniqueByPhase[state.schedule.phaseOfIndex(index)]++
}

func (state *receiverOverloadState) recordDuplicate(index uint64) {
	if index >= state.schedule.expected {
		return
	}
	state.duplicateByPhase[state.schedule.phaseOfIndex(index)]++
}

// finish takes the final observation of every association.
func (state *receiverOverloadState) finish() {
	if state.final != nil {
		return
	}
	state.observeQueues()
	state.final = make([]overloadAssociationObservation, len(state.associations))
	for index, association := range state.associations {
		stats := association.DataQueueStats()
		current := association.State()
		state.final[index] = overloadAssociationObservation{
			Index: index, QueueCapacity: stats.Capacity, MaxQueued: state.maxQueued[index],
			Discarded:          subtractFloor(stats.Discarded, state.discardBase[index]),
			CongestionEpisodes: state.episodes[index], CongestedAtEnd: stats.Congested,
			State: current.String(), Active: current == m3ua.StateASPActive,
			EpochStart: state.epochStart[index], EpochEnd: association.Epoch(),
		}
	}
}

func (state *receiverOverloadState) record() *overloadReceiverRecord {
	record := &overloadReceiverRecord{
		Scope:                   overloadReceiverScope,
		ConfiguredQueueCapacity: dataQueueSize,
		QueuePollInterval:       overloadQueuePollInterval,
		QueuePolls:              state.polls,
		UniqueByPhase:           append([]uint64(nil), state.uniqueByPhase...),
		DuplicateByPhase:        append([]uint64(nil), state.duplicateByPhase...),
		Misscoped:               state.misscoped,
		DeliveredLedger:         overloadLedgerSummary{Messages: state.schedule.expected, Delivered: state.delivered.count()},
		Series:                  append([]overloadReceiverPoint(nil), state.series...),
	}
	if state.final != nil {
		record.Associations = append([]overloadAssociationObservation(nil), state.final...)
		for _, association := range state.final {
			record.Discarded += association.Discarded
		}
	} else if state.begun {
		record.Discarded = state.discarded()
	}
	if peak, err := peakRSSBytes(); err != nil {
		record.PeakRSSError = err.Error()
	} else {
		record.PeakRSSBytes = peak
	}
	return record
}

// startOverloadLocked begins the overload observation of a cohort that has
// just started measuring. It runs under the control mutex.
func (control *receiverControl) startOverloadLocked() {
	state := control.overload
	if state == nil {
		return
	}
	associations := make([]*m3ua.Association, 0, len(control.tracked))
	for _, association := range control.tracked {
		if association != nil {
			associations = append(associations, association)
		}
	}
	state.begin(associations)
	state.stopPoll = make(chan struct{})
	go control.pollOverloadQueues(control.generation, state.stopPoll)
}

// stopOverloadLocked ends the queue poller of the current overload cohort, if
// one is running. It runs under the control mutex.
func (control *receiverControl) stopOverloadLocked() {
	if control.overload == nil || control.overload.stopPoll == nil {
		return
	}
	close(control.overload.stopPoll)
	control.overload.stopPoll = nil
}

func (control *receiverControl) pollOverloadQueues(generation uint64, stop <-chan struct{}) {
	ticker := time.NewTicker(overloadQueuePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			control.mutex.Lock()
			if control.generation == generation && control.phase == receiverMeasuring && control.overload != nil {
				control.overload.observeQueues()
			}
			control.mutex.Unlock()
		}
	}
}

// trackAssociation records the association serving a transport index, so an
// overload cohort can observe its inbound DATA queue and state.
func (control *receiverControl) trackAssociation(index int, association *m3ua.Association) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if index < 0 || index >= control.expectedAssociations {
		return
	}
	if control.tracked == nil {
		control.tracked = make([]*m3ua.Association, control.expectedAssociations)
	}
	control.tracked[index] = association
}

// overloadProgressLocked returns the overload fields of one /progress snapshot
// together with the delivery snapshot they bracket.
func (control *receiverControl) overloadProgressLocked() (*receiverOverloadProgress, ledgerSnapshot, bool) {
	state := control.overload
	before := state.discarded()
	snapshot, present := control.deliveryLocked()
	after := state.discarded()
	var queued uint64
	for _, association := range state.associations {
		queued += uint64(association.DataQueueStats().Queued)
	}
	return &receiverOverloadProgress{DiscardedBefore: before, DiscardedAfter: after, Queued: queued}, snapshot, present
}

// sampleOverloadLocked appends one per-second receiver overload point.
func (control *receiverControl) sampleOverloadLocked(offset time.Duration, unique uint64) {
	state := control.overload
	if state == nil || !state.begun || len(state.series) >= 601 {
		return
	}
	discarded, queued := state.observeQueues()
	maximum := 0
	for _, value := range state.maxQueued {
		maximum = max(maximum, value)
	}
	state.series = append(state.series, overloadReceiverPoint{
		OffsetMillis: uint64(offset / time.Millisecond), Unique: unique, Discarded: discarded, Queued: queued, MaxQueued: maximum,
	})
}

// serveDeliveredLedger returns the stopped overload cohort's per-message
// delivered ledger: one bit per scheduled message index, little-endian 64-bit
// words. It is served only once the cohort has stopped, and only for the
// generation the caller names.
func (control *receiverControl) serveDeliveredLedger(writer http.ResponseWriter, request *http.Request) {
	generation, err := strconv.ParseUint(request.URL.Query().Get("generation"), 10, 64)
	if err != nil {
		http.Error(writer, "generation is required", http.StatusBadRequest)
		return
	}
	control.mutex.Lock()
	if control.overload == nil || control.phase != receiverStopped || control.generation != generation {
		control.mutex.Unlock()
		http.Error(writer, "no stopped overload cohort of that generation", http.StatusConflict)
		return
	}
	encoded := control.overload.delivered.encode()
	control.mutex.Unlock()
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}
