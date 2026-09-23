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
// association index, reserved, sequence. The rest is a deterministic pattern
// the receiver checks, so a corrupted or truncated payload is invalid rather
// than counted.
const (
	payloadHeaderSize = 24
	payloadVersion    = 1
	kindLedger        = byte(1)
	kindOverload      = byte(2)
)

var payloadMagic = [4]byte{'M', '3', 'C', 'M'}

type payloadHeader struct {
	Kind        byte
	Epoch       uint32
	Association uint16
	Sequence    uint64
}

func payloadSize(kind byte) int {
	if kind == kindOverload {
		return overloadPayloadSize
	}
	return ledgerPayloadSize
}

func patternByte(sequence uint64, offset int) byte {
	return byte(uint64(offset)*7 + sequence)
}

func encodePayload(buffer []byte, header payloadHeader) []byte {
	size := payloadSize(header.Kind)
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
	buffer[14], buffer[15] = 0, 0
	binary.BigEndian.PutUint64(buffer[16:], header.Sequence)
	for offset := payloadHeaderSize; offset < size; offset++ {
		buffer[offset] = patternByte(header.Sequence, offset)
	}
	return buffer
}

var errInvalidPayload = errors.New("invalid fixture payload")

func decodePayload(data []byte) (payloadHeader, error) {
	if len(data) < payloadHeaderSize || [4]byte(data[:4]) != payloadMagic || data[4] != payloadVersion {
		return payloadHeader{}, fmt.Errorf("%w: header", errInvalidPayload)
	}
	header := payloadHeader{Kind: data[5], Epoch: binary.BigEndian.Uint32(data[8:]),
		Association: binary.BigEndian.Uint16(data[12:]), Sequence: binary.BigEndian.Uint64(data[16:])}
	if header.Kind != kindLedger && header.Kind != kindOverload {
		return payloadHeader{}, fmt.Errorf("%w: kind %d", errInvalidPayload, header.Kind)
	}
	if len(data) != payloadSize(header.Kind) || int(header.Association) >= stableAssociations {
		return payloadHeader{}, fmt.Errorf("%w: %d bytes for association %d", errInvalidPayload, len(data), header.Association)
	}
	for offset := payloadHeaderSize; offset < len(data); offset++ {
		if data[offset] != patternByte(header.Sequence, offset) {
			return payloadHeader{}, fmt.Errorf("%w: pattern at %d", errInvalidPayload, offset)
		}
	}
	return header, nil
}

// receiveLedger checks one direction of one epoch. Each stable association's
// DATA uses one SLS and therefore one ordered SCTP stream, so the ledger
// requires strict sequence order per association: a gap is loss, a repeat or
// a late arrival is duplication or reordering.
type receiveLedger struct {
	mutex    sync.Mutex
	epoch    uint32
	next     [stableAssociations]uint64
	unique   [stableAssociations]uint64
	gaps     uint64
	late     uint64
	invalid  uint64
	stale    uint64
	overload [stableAssociations]uint64
}

func (ledger *receiveLedger) reset(epoch uint32) {
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	ledger.epoch = epoch
	ledger.next, ledger.unique, ledger.overload = [stableAssociations]uint64{}, [stableAssociations]uint64{}, [stableAssociations]uint64{}
	ledger.gaps, ledger.late, ledger.invalid, ledger.stale = 0, 0, 0, 0
}

// record classifies one received payload; association is the stable index of
// the association it arrived on.
func (ledger *receiveLedger) record(association int, data []byte) {
	header, err := decodePayload(data)
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	switch {
	case err != nil || int(header.Association) != association:
		ledger.invalid++
	case header.Kind == kindOverload:
		ledger.overload[association]++
	case header.Epoch != ledger.epoch:
		ledger.stale++
	case header.Sequence == ledger.next[association]:
		ledger.unique[association]++
		ledger.next[association]++
	case header.Sequence > ledger.next[association]:
		ledger.gaps++
		ledger.unique[association]++
		ledger.next[association] = header.Sequence + 1
	default:
		ledger.late++
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

// reached reports whether every association has delivered at least sent.
func (ledger *receiveLedger) reached(sent []uint64) bool {
	unique := ledger.uniqueCounts()
	for index, count := range sent {
		if unique[index] < count {
			return false
		}
	}
	return true
}

// result compares the ledger with what the sender says it wrote.
func (ledger *receiveLedger) result(direction string, sent []uint64, writeErrors uint64, firstWriteError string) ledgerResult {
	ledger.mutex.Lock()
	defer ledger.mutex.Unlock()
	result := ledgerResult{Direction: direction, Epoch: ledger.epoch, Sent: append([]uint64(nil), sent...),
		Unique: append([]uint64(nil), ledger.unique[:]...), Gaps: ledger.gaps, Late: ledger.late,
		Invalid: ledger.invalid, Stale: ledger.stale, WriteErrors: writeErrors, FirstWriteError: firstWriteError}
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

// ledgerSender writes one epoch of ledgered DATA on every stable association
// at a fixed per-association rate, in its own goroutine per association.
type ledgerSender struct {
	towardSGP    bool
	epoch        uint32
	associations []*m3ua.Association
	sent         [stableAssociations]atomic.Uint64
	errors       atomic.Uint64
	refused      atomic.Uint64
	firstError   atomic.Pointer[string]
	cancel       context.CancelFunc
	group        sync.WaitGroup
}

func startLedgerSender(ctx context.Context, associations []*m3ua.Association, towardSGP bool, epoch uint32, perAssociation int) *ledgerSender {
	sender := &ledgerSender{towardSGP: towardSGP, epoch: epoch, associations: associations}
	ctx, sender.cancel = context.WithCancel(ctx)
	interval := time.Second / time.Duration(perAssociation)
	for index, association := range associations {
		sender.group.Add(1)
		go func(index int, association *m3ua.Association) {
			defer sender.group.Done()
			sender.run(ctx, index, association, interval)
		}(index, association)
	}
	return sender
}

func (sender *ledgerSender) run(ctx context.Context, index int, association *m3ua.Association, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	key, protocolData := dataTuple(index, sender.towardSGP)
	buffer := make([]byte, ledgerPayloadSize)
	for sequence := uint64(0); ; {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		protocolData.Data = encodePayload(buffer, payloadHeader{Kind: kindLedger, Epoch: sender.epoch,
			Association: uint16(index), Sequence: sequence})
		refusals, err := writeWithBackpressure(ctx, ledgerBackpressureWait, func() error {
			_, err := association.WriteData(m3ua.DataRequest{AS: key, ProtocolData: protocolData})
			return err
		})
		sender.refused.Add(uint64(refusals))
		if err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			// Stopped while the transport was refusing: nothing was sent.
			return
		}
		if err != nil {
			sender.errors.Add(1)
			message := fmt.Sprintf("association %d sequence %d: %v", index, sequence, err)
			sender.firstError.CompareAndSwap(nil, &message)
			// A failed write never advances the sequence: the ledger then
			// requires no gap for it, and the failure is counted instead.
			continue
		}
		sender.sent[index].Add(1)
		sequence++
	}
}

// ledgerBackpressureWait bounds how long one ledgered message may be refused
// for a full send buffer before it counts as a write failure.
const ledgerBackpressureWait = 5 * time.Second

// senderResult is what one epoch's sender wrote. Refused counts transport
// refusals that were offered again and then accepted or failed; a refusal is
// backpressure, never loss, because nothing of a refused message is queued.
type senderResult struct {
	Sent       []uint64
	Errors     uint64
	FirstError string
	Refused    uint64
}

// stop ends the epoch and returns what was written per association.
func (sender *ledgerSender) stop() senderResult {
	sender.cancel()
	sender.group.Wait()
	result := senderResult{Sent: make([]uint64, stableAssociations), Errors: sender.errors.Load(), Refused: sender.refused.Load()}
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
