// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// TestCancelledDATAIsAbsentAfterTransportResumes exercises the production
// send path against a peer that stops reading. The numbered payloads distinguish
// accepted traffic from each cancelled send after the peer resumes. A packet
// capture of this test can independently verify the same boundary.
func TestCancelledDATAIsAbsentAfterTransportResumes(t *testing.T) {
	for phase, name := range []string{"before admission", "behind control", "buffer wait"} {
		t.Run(name, func(t *testing.T) { testCancelledDATAAfterTransportResumes(t, phase, false, "") })
	}
}

// TestWriteDataContextCancelsKernelBufferWait rejects periodic NoWait retries:
// the real transport must own one cancellable buffer wait under the deadline.
func TestWriteDataContextCancelsKernelBufferWait(t *testing.T) {
	testCancelledDATAAfterTransportResumes(t, 2, true, "")
}

func TestWriteDataContextObservesKernelDeadlineChanges(t *testing.T) {
	for _, change := range []string{"remove", "expire", "far future"} {
		t.Run(change, func(t *testing.T) { testCancelledDATAAfterTransportResumes(t, 2, true, change) })
	}
}

type observedContextTransport struct {
	*sctp.Conn
	entered    chan struct{}
	once       sync.Once
	ordinary   atomic.Int32
	contextual atomic.Int32
	noWait     atomic.Bool
}

func (s *observedContextTransport) SendMsg(b []byte, opts sctp.SendOptions) (int, error) {
	s.ordinary.Add(1)
	s.once.Do(func() { close(s.entered) })
	return s.Conn.SendMsg(b, opts)
}

func (s *observedContextTransport) SendMsgContext(ctx context.Context, b []byte, opts sctp.SendOptions) (int, error) {
	s.contextual.Add(1)
	s.noWait.Store(opts.NoWait)
	s.once.Do(func() { close(s.entered) })
	return s.Conn.SendMsgContext(ctx, b, opts)
}

func testCancelledDATAAfterTransportResumes(t *testing.T, phase int, observeWait bool, deadlineChange string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := allocationRequest(0).AS
	config := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
	config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	config.ControlWriteTimeout = 30 * time.Second
	address := &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Port: uint16(3316 + phase)}
	sgp, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
	listener, err := sgp.Listen("m3ua", address, NewListenerConfig(config))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	accepted := make(chan *Association, 1)
	go func() {
		if c, err := listener.Accept(ctx); err == nil {
			accepted <- c
		}
	}()
	peer := dialStalledASP(t, address, 1)
	var c *Association
	select {
	case c = <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("SGP did not accept")
	}
	if err := c.sctpConn.SetWriteBuffer(stallingSocketBuffer); err != nil {
		t.Fatal(err)
	}
	request := DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
		OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2,
		Data: make([]byte, 1024),
	}}
	sent := 0
	if phase != 0 {
		// A first EAGAIN can be transient while packets move into the
		// peer's receive window. Require sustained refusal before the
		// cancellation experiment, while counting only accepted DATA.
		for refusals := 0; refusals < 16; {
			if sent == 200000 {
				t.Fatal("send buffer did not fill")
			}
			binary.BigEndian.PutUint64(request.ProtocolData.Data, uint64(sent))
			_, err := c.WriteData(request)
			if err == nil {
				sent++
				refusals = 0
				continue
			}
			if !errors.Is(err, syscall.EAGAIN) {
				t.Fatal(err)
			}
			refusals++
			time.Sleep(5 * time.Millisecond)
		}
	}
	var controlDone chan error
	var stopControl atomic.Bool
	if phase == 1 {
		controlDone = make(chan error, 1)
		go func() {
			for {
				_, err := c.writeControl(heartbeatAckForTest())
				if err != nil || stopControl.Load() {
					controlDone <- err
					return
				}
			}
		}()
		if !waitFor(func() bool { return libraryWriteWaiting(c) }, 5*time.Second) {
			t.Fatal("control did not park")
		}
	}
	originalDeadline := time.Now().Add(30 * time.Second)
	if deadlineChange != "" {
		originalDeadline = time.Now().Add(250 * time.Millisecond)
	}
	if phase == 2 {
		if err := c.SetWriteDeadline(originalDeadline); err != nil {
			t.Fatal(err)
		}
	}
	const cancelledBase uint64 = 0xc163000000000000
	const acceptedBase uint64 = 0xa163000000000000
	cancelledSequence := cancelledBase + uint64(phase)
	binary.BigEndian.PutUint64(request.ProtocolData.Data, cancelledSequence)
	callCtx, callCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	if phase == 0 {
		callCancel()
	}
	if observeWait {
		callCancel()
		callCtx, callCancel = context.WithCancel(ctx)
		defer callCancel()
		transport := &observedContextTransport{Conn: c.sctpConn, entered: make(chan struct{})}
		c.controlTransport = transport
		done := make(chan error, 1)
		go func() {
			_, sendErr := c.WriteDataContext(callCtx, request)
			done <- sendErr
		}()
		select {
		case <-transport.entered:
		case <-time.After(time.Second):
			t.Fatal("DATA did not enter the transport")
		}
		select {
		case err = <-done:
			t.Fatalf("DATA did not wait for buffer space: %v", err)
		case <-time.After(25 * time.Millisecond):
		}
		if deadlineChange != "" {
			var deadline time.Time
			switch deadlineChange {
			case "expire":
				deadline = time.Now().Add(-time.Second)
			case "far future":
				deadline = time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			if err := c.SetWriteDeadline(deadline); err != nil {
				t.Fatal(err)
			}
		}
		start := time.Now()
		if deadlineChange != "expire" {
			callCancel()
		}
		select {
		case err = <-done:
		case <-time.After(time.Second):
			t.Fatal("cancellation did not release the buffer wait")
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Errorf("cancellation took %v, want at most 100ms", elapsed)
		}
		if transport.ordinary.Load() != 0 || transport.contextual.Load() != 1 || transport.noWait.Load() {
			t.Errorf("buffer wait polled: SendMsg=%d SendMsgContext=%d NoWait=%v", transport.ordinary.Load(), transport.contextual.Load(), transport.noWait.Load())
		}
		t.Logf("buffer wait released in %v; SendMsg=%d SendMsgContext=%d", time.Since(start), transport.ordinary.Load(), transport.contextual.Load())
		if deadlineChange == "remove" || deadlineChange == "far future" {
			// Wait past the original bound: restoring it rather than the
			// caller's replacement would now refuse even a raw write probe.
			time.Sleep(time.Until(originalDeadline.Add(25 * time.Millisecond)))
			raw, rawErr := c.sctpConn.SyscallConn()
			if rawErr != nil {
				t.Fatal(rawErr)
			}
			if rawErr = raw.Write(func(uintptr) bool { return true }); rawErr != nil {
				t.Fatalf("cancellation lost the changed write deadline: %v", rawErr)
			}
		}
	} else {
		_, err = c.WriteDataContext(callCtx, request)
	}
	cause := context.DeadlineExceeded
	if phase == 0 || observeWait {
		cause = context.Canceled
	}
	if deadlineChange == "expire" {
		cause = os.ErrDeadlineExceeded
	}
	if err == nil {
		// Keep draining even after this failure so the late-submission
		// assertion, and a capture, can observe the unwanted DATA.
		t.Error("WriteDataContext accepted a cancelled message")
	} else {
		requireDataWriteError(t, err, DataNotSent, cause)
		if observeWait && cause != os.ErrDeadlineExceeded && errors.Is(err, os.ErrDeadlineExceeded) {
			t.Error("context interruption also matched the socket deadline")
		}
	}
	callCancel()
	if err := c.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	stopControl.Store(true)
	close(peer.resume)
	if controlDone != nil {
		select {
		case err := <-controlDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("control did not resume")
		}
	}
	if !waitFor(func() bool { return len(peer.delivered()) >= sent }, 20*time.Second) {
		t.Fatal("accepted flood did not drain")
	}
	positiveSequence := acceptedBase + uint64(phase)
	binary.BigEndian.PutUint64(request.ProtocolData.Data, positiveSequence)
	if _, err := c.WriteData(request); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return len(peer.delivered()) >= sent+1 }, 5*time.Second) {
		t.Fatal("positive fence did not arrive")
	}
	time.Sleep(100 * time.Millisecond)
	seen := make(map[uint64]int)
	for _, sequence := range peer.delivered() {
		seen[sequence]++
	}
	if seen[cancelledSequence] != 0 || seen[positiveSequence] != 1 || len(seen) != sent+1 {
		t.Fatalf("cancelled=%d positive=%d distinct=%d accepted=%d", seen[cancelledSequence], seen[positiveSequence], len(seen), sent+1)
	}
	for sequence := 0; sequence < sent; sequence++ {
		if seen[uint64(sequence)] != 1 {
			t.Fatalf("accepted DATA %d arrived %d times", sequence, seen[uint64(sequence)])
		}
	}
}
