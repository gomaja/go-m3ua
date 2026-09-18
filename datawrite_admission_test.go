// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// Acceptance bullet 3: binding withdrawal and write admission have one defined
// order.
//
// RFC 4666 Section 4.3.4.4 sends the ASP Inactive Ack only "after all traffic
// is halted", so withdrawal marks the binding first and only then waits for
// traffic already admitted under it. Admission is the mirror: a write takes the
// Application Server's barrier and re-reads the binding under it. The two
// together give the guarantee this test pins — no write is admitted to a scope
// that has already been withdrawn, and a write admitted before the withdrawal
// completes before the withdrawal returns.
func TestWithdrawnScopeAdmitsNoFurtherWriteData(t *testing.T) {
	listener, applicationServer, asp, _ := distributionFixture(t, params.TrafficModeLoadshare)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	asp.maxMessageStreamID = 4
	applicationServer.setASPState(asp, StateASPActive, time.Hour)

	scope := associationConfigASKey(asp.cfg, 1)
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var submissions atomic.Int64
	var started atomic.Bool
	asp.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		submissions.Add(1)
		if started.CompareAndSwap(false, true) {
			close(writeStarted)
			<-releaseWrite
		}
		return len(data), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteData(DataRequest{AS: scope, ProtocolData: simpleProtocolData("in flight")})
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case err := <-writeDone:
		t.Fatalf("the write finished before reaching the transport: %v", err)
	case <-time.After(time.Second):
		t.Fatal("the admitted write never reached the transport")
	}

	quiesced := make(chan func(), 1)
	go func() { quiesced <- listener.as.quiesceASPFor(asp, []uint32{1}) }()
	select {
	case <-quiesced:
		t.Fatal("withdrawal completed while a write it had admitted was still in flight")
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseWrite)
	if err := <-writeDone; err != nil {
		t.Fatalf("the admitted write failed: %v", err)
	}
	var notify func()
	select {
	case notify = <-quiesced:
	case <-time.After(time.Second):
		t.Fatal("withdrawal did not complete after the admitted write finished")
	}
	notify()

	before := submissions.Load()
	_, err := asp.WriteData(DataRequest{AS: scope, ProtocolData: simpleProtocolData("too late")})
	requireDataWriteError(t, err, DataNotSent, ErrRoutingContextNotActive)
	if got := submissions.Load(); got != before {
		t.Errorf("a write was admitted to the withdrawn scope: %d submissions after withdrawal", got-before)
	}
}

// The same contract under concurrency, with the race detector watching the
// state both sides read. Writes keep arriving while the binding is withdrawn;
// none of them may be submitted once withdrawal has returned.
func TestWriteAdmissionIsOrderedAgainstBindingWithdrawal(t *testing.T) {
	listener, applicationServer, asp, _ := distributionFixture(t, params.TrafficModeLoadshare)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	asp.maxMessageStreamID = 4
	applicationServer.setASPState(asp, StateASPActive, time.Hour)

	scope := associationConfigASKey(asp.cfg, 1)
	var withdrawn atomic.Bool
	var admittedAfterWithdrawal, accepted, refused atomic.Int64
	asp.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		if withdrawn.Load() {
			admittedAfterWithdrawal.Add(1)
		}
		return len(data), nil
	}

	stop := make(chan struct{})
	var writers sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := asp.WriteData(DataRequest{AS: scope, ProtocolData: simpleProtocolData("concurrent")})
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, ErrRoutingContextNotActive), errors.Is(err, ErrNotEstablished),
					errors.Is(err, ErrNoActiveASP):
					refused.Add(1)
				default:
					t.Errorf("WriteData: %v", err)
					return
				}
			}
		}()
	}

	// Let traffic actually flow before the binding is taken away.
	deadline := time.Now().Add(time.Second)
	for accepted.Load() < 50 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if accepted.Load() == 0 {
		close(stop)
		writers.Wait()
		t.Fatal("no write was ever admitted, so withdrawal proves nothing")
	}

	notify := listener.as.quiesceASPFor(asp, []uint32{1})
	withdrawn.Store(true)
	notify()

	// Keep writing well past the withdrawal.
	time.Sleep(50 * time.Millisecond)
	close(stop)
	writers.Wait()

	if got := admittedAfterWithdrawal.Load(); got != 0 {
		t.Errorf("%d writes were admitted after the binding was withdrawn", got)
	}
	if refused.Load() == 0 {
		t.Error("no write was refused after withdrawal; the writers stopped too early to prove anything")
	}
}

// The deliberate control for the test above: the same withdrawal-versus-write
// pattern with the shared state left unsynchronized, to show that the race
// detector does fire on it. It is skipped unless asked for, because a test that
// reports a data race by design cannot live in the ordinary suite.
//
// Run it with:
//
//	M3UA_RACE_CONTROL=1 go test -race -run TestUnsynchronizedWithdrawalIsDetected .
func TestUnsynchronizedWithdrawalIsDetected(t *testing.T) {
	if os.Getenv("M3UA_RACE_CONTROL") != "1" {
		t.Skip("control experiment: set M3UA_RACE_CONTROL=1 with -race to observe the detector fire")
	}

	// The unsynchronized stand-in for a binding: one goroutine withdraws it
	// while another admits writes against it.
	withdrawn := false
	admitted := 0
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; i < 100000; i++ {
			if !withdrawn {
				admitted++
			}
		}
	}()
	withdrawn = true
	writers.Wait()
	t.Logf("admitted %d writes against an unsynchronized binding", admitted)
}
