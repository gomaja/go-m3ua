package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// fakeWriter records every accepted DATA and refuses or fails on demand.
type fakeWriter struct {
	mutex    sync.Mutex
	accepted []m3ua.DataRequest
	failed   []m3ua.DataRequest
	refuse   int
	failAt   int
	calls    int
}

func (writer *fakeWriter) WriteData(request m3ua.DataRequest) (int, error) {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	writer.calls++
	if writer.refuse > 0 {
		writer.refuse--
		return 0, refusal(m3ua.DataNotSent)
	}
	if writer.failAt != 0 && writer.calls == writer.failAt {
		request.ProtocolData.Data = append([]byte(nil), request.ProtocolData.Data...)
		writer.failed = append(writer.failed, request)
		return 0, &m3ua.DataWriteError{Outcome: m3ua.DataSendIndeterminate, Err: errors.New("association ended")}
	}
	request.ProtocolData.Data = append([]byte(nil), request.ProtocolData.Data...)
	writer.accepted = append(writer.accepted, request)
	return len(request.ProtocolData.Data), nil
}

func TestScheduledMessages(t *testing.T) {
	if got := scheduledMessages(20000, 1500*time.Millisecond); got != 30000 {
		t.Fatalf("scheduled %d", got)
	}
	if got := scheduledMessages(625, 1599*time.Microsecond); got != 0 {
		t.Fatalf("scheduled %d before the first slot", got)
	}
}

// steppedClock is a schedule clock that moves only when the test advances it.
type steppedClock struct {
	mutex sync.Mutex
	at    time.Time
}

func (clock *steppedClock) now() time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.at
}

func (clock *steppedClock) advance(step time.Duration) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.at = clock.at.Add(step)
}

// offered counts the slots offered to the writer: accepted and failed writes.
// A refused write is offered again in the same slot.
func (writer *fakeWriter) offered() uint64 {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	return uint64(len(writer.accepted) + len(writer.failed))
}

func TestSenderOffersTheOpenLoopScheduleOnEveryFlow(t *testing.T) {
	fakes := make([]*fakeWriter, stableAssociations)
	writers := make([]dataWriter, stableAssociations)
	for index := range fakes {
		fakes[index] = &fakeWriter{}
		writers[index] = fakes[index]
	}
	fakes[3].refuse = 5
	fakes[7].failAt = 3
	const rate = 6400
	// The schedule runs on a stepped clock, so the stop lands at an exact
	// point of it. On the wall clock a stop catches the sender mid-wake and
	// comes up short by however long the host held it off the CPU, which on
	// shared CI runners has been tens of milliseconds.
	clock := &steppedClock{at: time.Now()}
	sender := startLedgerSenderWithClock(context.Background(), writers, senderPlan{Epoch: 9, Rate: rate, Workload: workloadMix, TowardSGP: true}, clock.now)
	perAssociation := float64(rate) / float64(len(writers))
	const step, steps = 100 * time.Millisecond, 5
	// Each step leaves every association a step's worth of slots behind,
	// which it must make up in a burst: never more than is due, and all of it.
	for elapsed := step; elapsed <= steps*step; elapsed += step {
		clock.advance(step)
		due := scheduledMessages(perAssociation, elapsed)
		deadline := time.Now().Add(10 * time.Second)
		for caughtUp := false; !caughtUp; {
			caughtUp = true
			for association, fake := range fakes {
				offered := fake.offered()
				if offered > due {
					t.Fatalf("association %d offered %d slots with %d due at %v", association, offered, due, elapsed)
				}
				caughtUp = caughtUp && offered == due
			}
			if !caughtUp && time.Now().After(deadline) {
				t.Fatalf("the sender did not catch up to %d slots per association at %v", due, elapsed)
			}
			time.Sleep(time.Millisecond)
		}
	}
	result := sender.stop()

	var sent uint64
	for _, count := range result.Sent {
		sent += count
	}
	if want := scheduledMessages(perAssociation, steps*step) * stableAssociations; result.Scheduled != want {
		t.Fatalf("scheduled %d at %d/s over %v of schedule, want %d", result.Scheduled, rate, steps*step, want)
	}
	if sent+result.Errors != result.Scheduled {
		t.Fatalf("sent %d with %d errors against %d scheduled", sent, result.Errors, result.Scheduled)
	}
	if result.Refused != 5 || result.Errors != 1 || result.Rate != rate || result.Workload != workloadMix {
		t.Fatalf("result refused %d errors %d rate %v workload %s", result.Refused, result.Errors, result.Rate, result.Workload)
	}
	for association, fake := range fakes {
		next := make([]uint64, flowsPerAssociation)
		for _, request := range fake.accepted {
			header, err := decodePayload(request.ProtocolData.Data)
			if err != nil || int(header.Association) != association || header.Epoch != 9 {
				t.Fatalf("association %d: %+v, %v", association, header, err)
			}
			flow := int(header.Flow)
			key, tuple := dataTuple(association, flow, true)
			if request.AS != key || request.ProtocolData.SignallingLinkSelection != tuple.SignallingLinkSelection ||
				request.ProtocolData.DestinationPointCode != tuple.DestinationPointCode {
				t.Fatalf("association %d flow %d: request %+v", association, flow, request.AS)
			}
			if header.Sequence != next[flow] || len(request.ProtocolData.Data) != workloadMix.size(header.Sequence) {
				t.Fatalf("association %d flow %d: sequence %d (want %d), %d bytes", association, flow, header.Sequence, next[flow],
					len(request.ProtocolData.Data))
			}
			next[flow]++
		}
		// Interleaving is judged on what was offered to each flow: a write that
		// failed was still that flow's turn, so it counts with the accepted
		// ones, and the stop may land anywhere in the round, so flows may
		// differ by one.
		attempted := append([]uint64(nil), next...)
		for _, request := range fake.failed {
			header, err := decodePayload(request.ProtocolData.Data)
			if err != nil {
				t.Fatalf("association %d: failed request %v", association, err)
			}
			attempted[header.Flow]++
		}
		for flow := range next {
			if next[flow] != result.Sent[flowIndex(association, flow)] {
				t.Fatalf("association %d flow %d: %d accepted, %d reported", association, flow, next[flow], result.Sent[flowIndex(association, flow)])
			}
			if attempted[flow]+1 < attempted[0] || attempted[flow] > attempted[0]+1 {
				t.Fatalf("association %d flows are not interleaved: %v attempted (%v accepted)", association, attempted, next)
			}
		}
	}
}
