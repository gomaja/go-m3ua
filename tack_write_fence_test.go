// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// retryMarshalProbe observes a retry after its outer stop check, before it
// attempts to acquire the control writer. It uses the real message encoder.
type retryMarshalProbe struct {
	messages.M3UA
	entered chan struct{}
}

func (p *retryMarshalProbe) MarshalTo(dst []byte) error {
	close(p.entered)
	return p.M3UA.MarshalTo(dst)
}

func startObservedRetry(t *testing.T, a *Association, message messages.M3UA, kind requestKind) (*pendingRequest, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	probe := &retryMarshalProbe{M3UA: message, entered: make(chan struct{})}
	request := &pendingRequest{msg: probe, kind: kind, stop: make(chan struct{}), result: make(chan error, 1), routingContexts: map[uint32]struct{}{2: {}}, remaining: map[uint32]struct{}{2: {}}}
	a.tack.mu.Lock()
	a.tack.pending[probe] = request
	a.tack.mu.Unlock()
	done := make(chan struct{})
	go func() { a.runTAck(request, kind); close(done) }()
	return request, probe.entered, done
}

func TestTAckRetryCannotWriteAfterCompletedWithdrawal(t *testing.T) {
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	a.cfg.TAck = 5 * time.Millisecond
	a.cfg.TAckRetries = 1
	a.cfg.ControlWriteTimeout = 5 * time.Second
	a.recvStream.Store(0)
	a.noteRoutingContextsAcked(params.NewRoutingContext(1))
	transport := newFullSendBuffer()
	transport.firstErr = nil
	a.signalWriter = nil
	a.controlTransport = transport
	a.transportCloser = transport.close
	inactive := messages.NewAspInactive(params.NewRoutingContext(2), nil)
	inactiveRequest := a.startTAck(inactive, requestAspInactive)
	inactiveRequest.stopRetries()
	if _, err := a.writeControl(inactive); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	transport.firstErr = syscall.EAGAIN
	transport.mu.Unlock()
	writerDone := make(chan error, 1)
	go func() { _, err := a.writeControl(heartbeatAckForTest()); writerDone <- err }()
	select {
	case <-transport.waiting:
	case <-time.After(time.Second):
		t.Fatal("other control write did not acquire the writer")
	}
	_, entered, retryDone := startObservedRetry(t, a, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(transport.space)
		t.Fatal("retry did not pass its outer stop check")
	}
	if err := a.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
		close(transport.space)
		t.Fatal(err)
	}
	// Withdrawal has completed while the retry still waits for the writer.
	close(transport.space)
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-retryDone:
	case <-time.After(time.Second):
		t.Fatal("stopped retry did not drain")
	}
	for _, raw := range transport.waitedFrames() {
		message, err := messages.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, active := message.(*messages.AspActive); active {
			t.Fatal("ASP Active was written after withdrawal completed")
		}
	}
}

func TestTAckRetryStoppedWhileWaitingForControlWriter(t *testing.T) {
	for _, test := range []struct {
		name    string
		kind    requestKind
		message messages.M3UA
	}{
		{"Up", requestAspUp, messages.NewAspUp(nil, nil)},
		{"Down", requestAspDown, messages.NewAspDown(nil)},
		{"Active", requestAspActive, messages.NewAspActive(nil, params.NewRoutingContext(2), nil)},
		{"Inactive", requestAspInactive, messages.NewAspInactive(params.NewRoutingContext(2), nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 2)
			a.cfg.TAck = 5 * time.Millisecond
			a.cfg.TAckRetries = 1
			transport := newFullSendBuffer()
			transport.firstErr = nil
			a.signalWriter = nil
			a.controlTransport = transport
			a.transportCloser = transport.close
			a.sendMu.Lock()
			request, entered, done := startObservedRetry(t, a, test.message, test.kind)
			select {
			case <-entered:
			case <-time.After(time.Second):
				a.sendMu.Unlock()
				t.Fatal("retry did not pass its outer stop check")
			}
			a.cancelTAckRequest(request)
			a.sendMu.Unlock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("stopped retry did not finish")
			}
			transport.mu.Lock()
			writes := len(transport.attempted)
			transport.mu.Unlock()
			if writes != 0 {
				t.Fatalf("stopped %s retry made %d transport submissions", test.name, writes)
			}
		})
	}
}

func TestStoppedDoubleExchangeDownDoesNotExpectAnAck(t *testing.T) {
	a, _ := newDoubleExchangeIPSPForTest(t)
	stopped := make(chan struct{})
	close(stopped)
	n, err := a.writeControlUntilStopped(messages.NewAspDown(nil), stopped)
	if err != nil || n != 0 {
		t.Fatalf("stopped Down returned %d, %v", n, err)
	}
	a.tack.mu.Lock()
	expected := a.tack.aspDownAcksOutstanding
	a.tack.mu.Unlock()
	if expected != 0 {
		t.Fatalf("skipped Down expects %d acknowledgements", expected)
	}
}

// stopOnFullControlSender stops the request after a NoWait refusal, before
// writeControlFrame can enter the blocking fallback.
type stopOnFullControlSender struct {
	*fullSendBuffer
	stop     func()
	blocking atomic.Int32
}

func (s *stopOnFullControlSender) SendMsg(frame []byte, options sctp.SendOptions) (int, error) {
	if options.NoWait {
		s.stop()
		return 0, syscall.EAGAIN
	}
	s.blocking.Add(1)
	return len(frame), nil
}
func TestTAckStoppedAfterNoWaitDoesNotStartBlockingSend(t *testing.T) {
	a, _ := newTestConnWithContexts(t, StateASPDown, RoleASP)
	a.cfg.TAck = time.Millisecond
	a.cfg.TAckRetries = 1
	message := messages.NewAspUp(nil, nil)
	request := &pendingRequest{msg: message, kind: requestAspUp, stop: make(chan struct{}), result: make(chan error, 1)}
	a.tack.pending[message] = request
	transport := &stopOnFullControlSender{fullSendBuffer: newFullSendBuffer(), stop: request.stopRetries}
	a.signalWriter = nil
	a.controlTransport = transport
	a.transportCloser = transport.close
	a.runTAck(request, requestAspUp)
	if calls := transport.blocking.Load(); calls != 0 {
		t.Fatalf("stopped retry started %d blocking sends", calls)
	}
}
