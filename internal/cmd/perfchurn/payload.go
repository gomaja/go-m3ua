package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua"
)

// DATA payload header: magic, version, kind, reserved, epoch, stable
// association index, flow, reserved, sequence. The rest is a deterministic
// pattern the receiver checks, so a corrupted or truncated payload is invalid
// rather than counted.
const (
	payloadHeaderSize = 24
	payloadVersion    = 2
	kindLedger        = byte(1)
	kindOverload      = byte(2)
)

var payloadMagic = [4]byte{'M', '3', 'C', 'M'}

type payloadHeader struct {
	Kind        byte
	Epoch       uint32
	Association uint16
	Flow        uint8
	Sequence    uint64
}

func patternByte(sequence uint64, offset int) byte {
	return byte(uint64(offset)*7 + sequence)
}

// encodePayload writes one payload of size bytes into buffer, growing it if
// needed.
func encodePayload(buffer []byte, header payloadHeader, size int) []byte {
	size = max(size, payloadHeaderSize)
	if cap(buffer) < size {
		buffer = make([]byte, size)
	}
	buffer = buffer[:size]
	copy(buffer, payloadMagic[:])
	buffer[4] = payloadVersion
	buffer[5] = header.Kind
	buffer[6], buffer[7] = 0, 0
	binary.BigEndian.PutUint32(buffer[8:], header.Epoch)
	binary.BigEndian.PutUint16(buffer[12:], header.Association)
	buffer[14], buffer[15] = header.Flow, 0
	binary.BigEndian.PutUint64(buffer[16:], header.Sequence)
	for offset := payloadHeaderSize; offset < size; offset++ {
		buffer[offset] = patternByte(header.Sequence, offset)
	}
	return buffer
}

var errInvalidPayload = errors.New("invalid fixture payload")

// decodePayload validates everything a payload says about itself. Whether a
// ledgered payload has the size its workload schedules for its sequence is
// the ledger's check, since only the ledger knows the workload.
func decodePayload(data []byte) (payloadHeader, error) {
	if len(data) < payloadHeaderSize || [4]byte(data[:4]) != payloadMagic || data[4] != payloadVersion {
		return payloadHeader{}, fmt.Errorf("%w: header", errInvalidPayload)
	}
	header := payloadHeader{Kind: data[5], Epoch: binary.BigEndian.Uint32(data[8:]),
		Association: binary.BigEndian.Uint16(data[12:]), Flow: data[14], Sequence: binary.BigEndian.Uint64(data[16:])}
	switch {
	case header.Kind != kindLedger && header.Kind != kindOverload:
		return payloadHeader{}, fmt.Errorf("%w: kind %d", errInvalidPayload, header.Kind)
	case header.Kind == kindOverload && len(data) != overloadPayloadSize:
		return payloadHeader{}, fmt.Errorf("%w: %d-byte overload payload", errInvalidPayload, len(data))
	case int(header.Association) >= stableAssociations || int(header.Flow) >= flowsPerAssociation:
		return payloadHeader{}, fmt.Errorf("%w: association %d flow %d", errInvalidPayload, header.Association, header.Flow)
	}
	for offset := payloadHeaderSize; offset < len(data); offset++ {
		if data[offset] != patternByte(header.Sequence, offset) {
			return payloadHeader{}, fmt.Errorf("%w: pattern at %d", errInvalidPayload, offset)
		}
	}
	return header, nil
}

// ledgerCounts are the counters a ledger accumulates for its epoch.
type ledgerCounts struct {
	unique, gaps, late, invalid, stale uint64
}

// ledgerAddendum is what a closing epoch recorded after it was judged: every
// arrival between result and the next reset. It is folded back into that
// epoch's result, so a late duplicate or a reordered straggler is judged
// against the epoch it belongs to instead of vanishing with the reset.
type ledgerAddendum struct {
	Epoch   uint32 `json:"epoch"`
	Unique  uint64 `json:"unique"`
	Gaps    uint64 `json:"gaps"`
	Late    uint64 `json:"duplicate_or_late"`
	Invalid uint64 `json:"invalid"`
	Stale   uint64 `json:"stale_epoch"`
}

// fold adds an addendum of this result's epoch. An arrival after the epoch
// was judged is a delivery that escaped its ledger.
func (result *ledgerResult) fold(addendum ledgerAddendum) {
	if addendum.Epoch == 0 || addendum.Epoch != result.Epoch {
		return
	}
	result.AfterClose += addendum.Unique
	result.Gaps += addendum.Gaps
	result.Late += addendum.Late
	result.Invalid += addendum.Invalid
	result.Stale += addendum.Stale
}

// receiveLedger checks one direction of one epoch. Each flow is on one SLS
// and therefore one ordered SCTP stream, so the ledger requires strict
// sequence order per flow: a gap is loss, a repeat or a late arrival is
// duplication or reordering. Ledgered payloads must have the size the epoch's
// workload schedules for their sequence.
type receiveLedger struct {
	mutex    sync.Mutex
	epoch    uint32
	workload payloadWorkload
	next     [flowCount]uint64
	unique   [flowCount]uint64
	counts   ledgerCounts
	overload [stableAssociations]uint64
	judged   bool
	atJudged ledgerCounts
}

// reset starts a new epoch and returns what the closing one recorded after it
// was judged; an epoch that was never judged has nothing to fold.
func (ledger *receiveLedger) reset(epoch uint32, workload payloadWorkload) ledgerAddendum {
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	addendum := ledger.addendumLocked()
	ledger.epoch, ledger.workload = epoch, workload
	ledger.next, ledger.unique, ledger.overload = [flowCount]uint64{}, [flowCount]uint64{}, [stableAssociations]uint64{}
	ledger.counts, ledger.atJudged, ledger.judged = ledgerCounts{}, ledgerCounts{}, false
	return addendum
}

// finalRead returns the closing addendum of the last epoch without starting
// another.
func (ledger *receiveLedger) finalRead() ledgerAddendum {
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	return ledger.addendumLocked()
}

func (ledger *receiveLedger) addendumLocked() ledgerAddendum {
	if !ledger.judged {
		return ledgerAddendum{}
	}
	now, then := ledger.counts, ledger.atJudged
	return ledgerAddendum{Epoch: ledger.epoch, Unique: now.unique - then.unique, Gaps: now.gaps - then.gaps,
		Late: now.late - then.late, Invalid: now.invalid - then.invalid, Stale: now.stale - then.stale}
}

// record classifies one received payload; association is the stable index of
// the association it arrived on.
func (ledger *receiveLedger) record(association int, data []byte) {
	header, err := decodePayload(data)
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	switch {
	case err != nil || int(header.Association) != association:
		ledger.counts.invalid++
		return
	case header.Kind == kindOverload:
		ledger.overload[association]++
		return
	case header.Epoch != ledger.epoch:
		ledger.counts.stale++
		return
	case len(data) != ledger.workload.size(header.Sequence):
		ledger.counts.invalid++
		return
	}
	flow := flowIndex(association, int(header.Flow))
	switch {
	case header.Sequence == ledger.next[flow]:
		ledger.unique[flow]++
		ledger.counts.unique++
		ledger.next[flow]++
	case header.Sequence > ledger.next[flow]:
		ledger.counts.gaps++
		ledger.unique[flow]++
		ledger.counts.unique++
		ledger.next[flow] = header.Sequence + 1
	default:
		ledger.counts.late++
	}
}

func (ledger *receiveLedger) uniqueCounts() []uint64 {
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	return append([]uint64(nil), ledger.unique[:]...)
}

func (ledger *receiveLedger) overloadTotal() uint64 {
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	total := uint64(0)
	for _, count := range ledger.overload {
		total += count
	}
	return total
}

// reached reports whether every flow has delivered at least sent.
func (ledger *receiveLedger) reached(sent []uint64) bool {
	unique := ledger.uniqueCounts()
	for index, count := range sent {
		if index >= len(unique) || unique[index] < count {
			return false
		}
	}
	return true
}

// result judges the epoch against what the sender says it wrote per flow,
// and marks the point after which arrivals are folded back as an addendum.
func (ledger *receiveLedger) result(direction string, sent []uint64, writeErrors uint64, firstWriteError string) ledgerResult {
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	ledger.judged, ledger.atJudged = true, ledger.counts
	result := ledgerResult{Direction: direction, Epoch: ledger.epoch, Workload: string(ledger.workload),
		Sent: append([]uint64(nil), sent...), Unique: append([]uint64(nil), ledger.unique[:]...),
		Gaps: ledger.counts.gaps, Late: ledger.counts.late, Invalid: ledger.counts.invalid, Stale: ledger.counts.stale,
		WriteErrors: writeErrors, FirstWriteError: firstWriteError}
	for index := range ledger.unique {
		var wrote uint64
		if index < len(sent) {
			wrote = sent[index]
		}
		result.SentTotal += wrote
		result.UniqueTotal += ledger.unique[index]
		if ledger.unique[index] < wrote {
			result.Missing += wrote - ledger.unique[index]
		} else {
			result.Excess += ledger.unique[index] - wrote
		}
	}
	return result
}

// waitLedger polls until the ledger reaches sent or the wait ends.
func waitLedger(ctx context.Context, ledger *receiveLedger, sent []uint64, wait time.Duration) {
	deadline := time.Now().Add(wait)
	for !ledger.reached(sent) && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
}

// dataWriter is the one Association method the sender uses.
type dataWriter interface {
	WriteData(request m3ua.DataRequest) (int, error)
}

// senderPlan is one epoch of ledgered traffic in one direction: Rate is the
// aggregate across every stable association.
type senderPlan struct {
	Epoch     uint32
	Rate      float64
	Workload  payloadWorkload
	TowardSGP bool
}

// scheduledMessages is how many messages an open-loop schedule at rate has
// released after elapsed.
func scheduledMessages(rate float64, elapsed time.Duration) uint64 {
	if rate <= 0 || elapsed <= 0 {
		return 0
	}
	return uint64(rate * elapsed.Seconds())
}

// senderWake bounds how long the sender sleeps between releases, so a slow
// wake-up costs at most this much of the schedule before it catches up.
const senderWake = 2 * time.Millisecond

// ledgeredBackpressureWait bounds how long one ledgered message may be refused
// for a full send buffer before it counts as a write failure.
const ledgeredBackpressureWait = 5 * time.Second

// ledgerSender offers one epoch of ledgered DATA on every stable association
// on an open-loop schedule: slot k of an association is due at k/rate and is
// sent then, however long earlier slots took, so a sender that falls behind
// catches up in a burst and a sender that cannot keep up shows as a shortfall
// against the schedule rather than as a slower rate. Slot k carries flow
// k mod flowsPerAssociation, so the flows interleave on the association.
type ledgerSender struct {
	plan       senderPlan
	writers    []dataWriter
	sent       [flowCount]atomic.Uint64
	errors     atomic.Uint64
	refused    atomic.Uint64
	firstError atomic.Pointer[string]
	// now is the schedule's clock: time.Now, or a test's stepped clock.
	now     func() time.Time
	started time.Time
	cancel  context.CancelFunc
	group   sync.WaitGroup
}

func startLedgerSender(ctx context.Context, writers []dataWriter, plan senderPlan) *ledgerSender {
	return startLedgerSenderWithClock(ctx, writers, plan, time.Now)
}

// startLedgerSenderWithClock is startLedgerSender with the schedule read from
// now, so a test can place the stop at an exact point of the schedule.
func startLedgerSenderWithClock(ctx context.Context, writers []dataWriter, plan senderPlan, now func() time.Time) *ledgerSender {
	sender := &ledgerSender{plan: plan, writers: writers, now: now, started: now()}
	ctx, sender.cancel = context.WithCancel(ctx)
	perAssociation := plan.Rate / float64(len(writers))
	for index, writer := range writers {
		sender.group.Add(1)
		go func(index int, writer dataWriter) {
			defer sender.group.Done()
			sender.run(ctx, index, writer, perAssociation)
		}(index, writer)
	}
	return sender
}

func (sender *ledgerSender) run(ctx context.Context, index int, writer dataWriter, rate float64) {
	var keys [flowsPerAssociation]m3ua.ASKey
	var tuples [flowsPerAssociation]m3ua.DataRequest
	for flow := range tuples {
		keys[flow], tuples[flow].ProtocolData = dataTuple(index, flow, sender.plan.TowardSGP)
		tuples[flow].AS = keys[flow]
	}
	var sequences [flowsPerAssociation]uint64
	buffer := make([]byte, overloadPayloadSize)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for slot := uint64(0); ; {
		due := scheduledMessages(rate, sender.now().Sub(sender.started))
		for ; slot < due; slot++ {
			if ctx.Err() != nil {
				return
			}
			flow := int(slot % flowsPerAssociation)
			request := tuples[flow]
			sequence := sequences[flow]
			request.ProtocolData.Data = encodePayload(buffer, payloadHeader{Kind: kindLedger, Epoch: sender.plan.Epoch,
				Association: uint16(index), Flow: uint8(flow), Sequence: sequence}, sender.plan.Workload.size(sequence))
			refusals, err := writeWithBackpressure(ctx, ledgeredBackpressureWait, func() error {
				_, err := writer.WriteData(request)
				return err
			})
			sender.refused.Add(uint64(refusals))
			if err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				// Stopped while the transport was refusing: nothing was sent.
				return
			}
			if err != nil {
				sender.errors.Add(1)
				message := fmt.Sprintf("association %d flow %d sequence %d: %v", index, flow, sequence, err)
				sender.firstError.CompareAndSwap(nil, &message)
				// A failed write never advances its flow: the next slot of
				// the flow offers the same sequence, so the ledger requires
				// no gap for it and the failure is counted instead.
				continue
			}
			sender.sent[flowIndex(index, flow)].Add(1)
			sequences[flow]++
		}
		next := sender.started.Add(time.Duration(float64(slot+1) / rate * float64(time.Second)))
		timer.Reset(min(max(next.Sub(sender.now()), 0), senderWake))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

// senderResult is what one epoch's sender wrote. Sent is per flow index.
// Scheduled is the open-loop schedule at the moment the sender stopped.
// Refused counts transport refusals that were offered again: backpressure,
// not loss, because nothing of a refused message is queued.
type senderResult struct {
	Sent       []uint64
	Scheduled  uint64
	Rate       float64
	Workload   payloadWorkload
	Errors     uint64
	FirstError string
	Refused    uint64
}

// stop ends the epoch and returns what was written.
func (sender *ledgerSender) stop() senderResult {
	elapsed := sender.now().Sub(sender.started)
	sender.cancel()
	sender.group.Wait()
	perAssociation := sender.plan.Rate / float64(len(sender.writers))
	result := senderResult{Sent: make([]uint64, flowCount), Rate: sender.plan.Rate, Workload: sender.plan.Workload,
		Errors: sender.errors.Load(), Refused: sender.refused.Load(),
		Scheduled: scheduledMessages(perAssociation, elapsed) * uint64(len(sender.writers))}
	for index := range result.Sent {
		result.Sent[index] = sender.sent[index].Load()
	}
	if pointer := sender.firstError.Load(); pointer != nil {
		result.FirstError = *pointer
	}
	return result
}

// drainChannels reads an association's indication channels until the library
// closes them. StateChanges and ManagementIndications close the association
// with ErrIndicationQueueFull if left unread, so every association has a
// reader for its whole life. onEnd, when set, runs once they are all closed,
// which is when the association has ended.
func drainChannels(association *m3ua.Association, onEnd func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if onEnd != nil {
			defer onEnd()
		}
		states, indications, statuses := association.StateChanges(), association.ManagementIndications(), association.SignallingStatus()
		for states != nil || indications != nil || statuses != nil {
			select {
			case _, open := <-states:
				if !open {
					states = nil
				}
			case _, open := <-indications:
				if !open {
					indications = nil
				}
			case _, open := <-statuses:
				if !open {
					statuses = nil
				}
			}
		}
	}()
	return done
}
