// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 Section 4.7 makes the SGP withdraw an affected AS with ASP
// Inactive Ack. Section 4.3.4.4 requires the Ack only after all traffic has
// stopped; a partial NIF failure cannot let an already-admitted direct DATA
// write escape after that Ack.
func TestPartialNIFIsolationWaitsForScopedDirectDataBeforeAspInactiveAck(t *testing.T) {
	listener, applicationServer, asp, _ := distributionFixture(t, params.TrafficModeLoadshare)
	listener.conns = map[*Association]struct{}{asp: {}}
	asp.listener = listener
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseWrite)
		}
	}()
	acknowledged := make(chan struct{})
	var acked atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.Data:
			close(writeStarted)
			<-releaseWrite
		case *messages.AspInactiveAck:
			if acked.CompareAndSwap(false, true) {
				close(acknowledged)
			}
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteSignal(distributionData(1, 3, "in flight"))
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case err := <-writeDone:
		t.Fatalf("direct DATA returned before SCTP write: %v", err)
	case <-time.After(time.Second):
		t.Fatal("direct DATA did not reach the writer")
	}

	isolationDone := make(chan struct{})
	go func() {
		listener.SetASAvailable(1, false)
		close(isolationDone)
	}()
	select {
	case <-acknowledged:
		t.Fatal("ASP Inactive Ack overtook an in-flight scoped DATA write")
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseWrite)
	released = true
	if err := <-writeDone; err != nil {
		t.Fatalf("direct DATA: %v", err)
	}
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("ASP Inactive Ack was not written after scoped DATA drained")
	}
	select {
	case <-isolationDone:
	case <-time.After(time.Second):
		t.Fatal("SetASAvailable did not finish")
	}
	if _, err := asp.WriteSignal(distributionData(1, 3, "after isolation")); err == nil {
		t.Fatal("direct DATA was accepted after partial NIF isolation")
	}
}

// Total NIF isolation applies to every AS on the association, including a
// dedicated connection with no Routing Context. Its ASP Down Ack has the same
// halt-before-Ack ordering requirement as the scoped partial-failure path.
func TestTotalNIFIsolationWaitsForUnscopedDirectDataBeforeAspDownAck(t *testing.T) {
	asp, _ := newTestConn(t, StateASPActive, RoleSGP)
	asp.maxMessageStreamID = 4
	asp.cfg.RoutingContexts = nil
	listener := &Listener{
		AssociationConfig: asp.cfg,
		conns:             map[*Association]struct{}{asp: {}},
	}
	asp.listener = listener

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseWrite)
		}
	}()
	asp.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(writeStarted)
		<-releaseWrite
		return len(data), nil
	}
	acknowledged := make(chan struct{})
	var acked atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspDownAck); ok && acked.CompareAndSwap(false, true) {
			close(acknowledged)
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := asp.WriteToStream([]byte("in flight"), 1)
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case err := <-writeDone:
		t.Fatalf("direct DATA returned before SCTP write: %v", err)
	case <-time.After(time.Second):
		t.Fatal("direct DATA did not reach the writer")
	}

	isolationDone := make(chan struct{})
	go func() {
		listener.SetNIFAvailable(false)
		close(isolationDone)
	}()
	select {
	case <-acknowledged:
		t.Fatal("ASP Down Ack overtook an in-flight unscoped DATA write")
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseWrite)
	released = true
	if err := <-writeDone; err != nil {
		t.Fatalf("direct DATA: %v", err)
	}
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("ASP Down Ack was not written after unscoped DATA drained")
	}
	select {
	case <-isolationDone:
	case <-time.After(time.Second):
		t.Fatal("SetNIFAvailable did not finish")
	}
}
