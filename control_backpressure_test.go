// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// fullSendBuffer models go-sctp's two sends against a socket whose send buffer
// is full: SCTPWrite without a deadline refuses with EAGAIN, and Write parks
// until space appears or the association is closed, which evicts it the way
// closing the descriptor evicts a write parked in the runtime poller.
type fullSendBuffer struct {
	mu        sync.Mutex
	firstErr  error
	infos     []sctp.SndRcvInfo
	attempted [][]byte
	waited    [][]byte
	waiting   chan struct{}
	space     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newFullSendBuffer() *fullSendBuffer {
	return &fullSendBuffer{
		firstErr: syscall.EAGAIN,
		waiting:  make(chan struct{}, 16),
		space:    make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

func (f *fullSendBuffer) SCTPWrite(b []byte, info *sctp.SndRcvInfo) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempted = append(f.attempted, bytes.Clone(b))
	if info != nil {
		f.infos = append(f.infos, *info)
	}
	if f.firstErr != nil {
		return 0, f.firstErr
	}
	return len(b), nil
}

func (f *fullSendBuffer) Write(b []byte) (int, error) {
	f.waiting <- struct{}{}
	select {
	case <-f.space:
		f.mu.Lock()
		f.waited = append(f.waited, bytes.Clone(b))
		f.mu.Unlock()
		return len(b), nil
	case <-f.closed:
		return 0, net.ErrClosed
	}
}

func (f *fullSendBuffer) close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

func (f *fullSendBuffer) waitedFrames() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.waited...)
}

// newBackpressureTestConn returns an ASP-ACTIVE SGP association whose signals
// reach the given transport rather than a test recorder.
func newBackpressureTestConn(t *testing.T, transport *fullSendBuffer, timeout time.Duration) *Association {
	t.Helper()
	c, _ := newTestConn(t, StateASPActive, RoleSGP)
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	c.cfg.ControlWriteTimeout = timeout
	return c
}

func heartbeatAckForTest() *messages.HeartbeatAck {
	return messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("backpressure")))
}

func TestLibraryWriteWaitsOutAFullSendBuffer(t *testing.T) {
	transport := newFullSendBuffer()
	c := newBackpressureTestConn(t, transport, time.Minute)

	result := make(chan error, 1)
	go func() {
		_, err := c.writeControl(heartbeatAckForTest())
		result <- err
	}()

	select {
	case <-transport.waiting:
	case err := <-result:
		t.Fatalf("the library write returned %v instead of waiting for send-buffer space", err)
	case <-time.After(time.Second):
		t.Fatal("the library write neither waited nor returned")
	}
	select {
	case err := <-result:
		t.Fatalf("the library write returned %v while the send buffer was still full", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(transport.space)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("the library write failed after space appeared: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the library write did not complete after space appeared")
	}
	select {
	case <-c.Done():
		t.Fatalf("a full send buffer closed the association: %v", c.Err())
	default:
	}

	// The waiting send carries no ancillary data, so it must be the very
	// message the refused attempt tried to send, and that attempt must have
	// named the control template the socket defaults are set to.
	waited := transport.waitedFrames()
	if len(waited) != 1 || len(transport.attempted) != 1 || !bytes.Equal(waited[0], transport.attempted[0]) {
		t.Fatalf("waited frames %x after attempts %x; want the refused frame once", waited, transport.attempted)
	}
	if got := transport.infos[0]; got.Stream != 0 || got.PPID != M3UAPPID {
		t.Fatalf("the refused attempt used stream %d PPID %d; want the control template 0/%d", got.Stream, got.PPID, M3UAPPID)
	}
}

func TestLibraryWriteClosesTheAssociationWhenTheWaitExceedsTheBound(t *testing.T) {
	const bound = 100 * time.Millisecond
	transport := newFullSendBuffer()
	c := newBackpressureTestConn(t, transport, bound)

	started := time.Now()
	_, err := c.writeControl(heartbeatAckForTest())
	elapsed := time.Since(started)

	if !errors.Is(err, ErrControlWriteTimeout) {
		t.Fatalf("the library write returned %v; want ErrControlWriteTimeout", err)
	}
	if elapsed < bound {
		t.Fatalf("the library write gave up after %v, before the %v bound", elapsed, bound)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("the association stayed open after the control write timed out")
	}
	if !errors.Is(c.Err(), ErrControlWriteTimeout) {
		t.Fatalf("the association ended with %v; want ErrControlWriteTimeout", c.Err())
	}
}

func TestApplicationWriteSignalReportsAFullSendBufferWithoutWaiting(t *testing.T) {
	transport := newFullSendBuffer()
	c := newBackpressureTestConn(t, transport, time.Minute)

	result := make(chan error, 1)
	go func() {
		_, err := c.WriteSignal(heartbeatAckForTest())
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("WriteSignal returned %v; want the transport's EAGAIN", err)
		}
	case <-transport.waiting:
		t.Fatal("WriteSignal waited for send-buffer space; the application owns its backpressure")
	case <-time.After(time.Second):
		t.Fatal("WriteSignal did not return")
	}
	select {
	case <-c.Done():
		t.Fatalf("a full send buffer on an application write closed the association: %v", c.Err())
	default:
	}
}

func TestLibraryWriteReportsOtherTransportErrorsWithoutWaiting(t *testing.T) {
	transport := newFullSendBuffer()
	transport.firstErr = syscall.EPIPE
	const bound = 50 * time.Millisecond
	c := newBackpressureTestConn(t, transport, bound)

	_, err := c.writeControl(heartbeatAckForTest())
	if !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("the library write returned %v; want the transport's EPIPE", err)
	}
	select {
	case <-transport.waiting:
		t.Fatal("a failed send was waited on as if the buffer were full")
	default:
	}
	// No watchdog may be left behind to close the association later.
	time.Sleep(3 * bound)
	select {
	case <-c.Done():
		t.Fatalf("the association was closed after a non-backpressure failure: %v", c.Err())
	default:
	}
}

func TestMandatoryControlQueueSurvivesAFullSendBuffer(t *testing.T) {
	transport := newFullSendBuffer()
	c := newBackpressureTestConn(t, transport, time.Minute)
	c.notificationQueue = make(chan mandatoryControl, defaultNotificationQueueSize)
	t.Cleanup(func() { _ = c.Close() })

	result := make(chan error, 1)
	go func() {
		result <- c.writeMandatoryControls([]messages.M3UA{heartbeatAckForTest()}, false, true)
	}()
	select {
	case <-transport.waiting:
	case err := <-result:
		t.Fatalf("the queued control write returned %v instead of waiting", err)
	case <-time.After(time.Second):
		t.Fatal("the queued control write neither waited nor returned")
	}
	close(transport.space)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("the queued control write failed after space appeared: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the queued control write did not complete after space appeared")
	}
	select {
	case <-c.Done():
		t.Fatalf("the mandatory control worker closed the association on a full send buffer: %v", c.Err())
	default:
	}
}

func TestControlWriteTimeoutDefaults(t *testing.T) {
	for _, configured := range []time.Duration{0, -time.Second} {
		c := &Association{cfg: &AssociationConfig{ControlWriteTimeout: configured}}
		if got := c.controlWriteTimeout(); got != DefaultControlWriteTimeout {
			t.Errorf("ControlWriteTimeout %v resolved to %v; want DefaultControlWriteTimeout", configured, got)
		}
	}
	c := &Association{cfg: &AssociationConfig{ControlWriteTimeout: 750 * time.Millisecond}}
	if got := c.controlWriteTimeout(); got != 750*time.Millisecond {
		t.Errorf("ControlWriteTimeout 750ms resolved to %v", got)
	}
	if got := (&Association{}).controlWriteTimeout(); got != DefaultControlWriteTimeout {
		t.Errorf("an Association without configuration resolved %v", got)
	}
}
