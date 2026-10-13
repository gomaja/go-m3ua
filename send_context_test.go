// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func setContextTestWriteDeadline(c *Association, deadline time.Time) {
	if deadline.IsZero() {
		c.applicationWriteDeadline.Store(nil)
	} else {
		c.applicationWriteDeadline.Store(&deadline)
	}
	if b, ok := c.controlTransport.(*contextSendBuffer); ok {
		select {
		case b.changed <- struct{}{}:
		default:
		}
	}
}

func TestWriteDataContextCancelledBeforeAdmission(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := c.WriteDataContext(ctx, allocationRequest(128))
	if n != 0 {
		t.Fatalf("octets = %d, want 0", n)
	}
	requireDataWriteError(t, err, DataNotSent, context.Canceled)
	if capture.submissions() != 0 {
		t.Fatal("cancelled DATA reached the transport")
	}
	// A cancelled call is rejected before state or request validation, and
	// before an encoded buffer or a barrier can be needed.
	c.state = StateASPInactive
	_, err = c.WriteDataContext(ctx, allocationRequest(128))
	requireDataWriteError(t, err, DataNotSent, context.Canceled)
}

func TestWriteDataContextCancelsScopeAdmission(t *testing.T) {
	for _, role := range []Role{RoleSGP, RoleIPSP} {
		t.Run(role.String(), func(t *testing.T) {
			var c *Association
			var barrier *contextMutex
			var scope ASKey
			if role == RoleSGP {
				_, as, association, _ := distributionFixture(t, params.TrafficModeLoadshare)
				c = association
				c.noteRoutingContextsActive([]uint32{1})
				c.setState(StateASPActive)
				c.maxMessageStreamID = 4
				as.setASPState(c, StateASPActive, time.Hour)
				barrier, scope = &as.deliveryMu, associationConfigASKey(c.cfg, 1)
			} else {
				c, _ = newDataWriteAssociation(t, 1)
				c.role = RoleIPSP
				c.noteRoutingContextsActive([]uint32{1})
				barrier, scope = &c.unscopedDeliveryMu, allocationRequest(0).AS
			}
			capture := &dataFrameCapture{}
			c.dataWriter = capture.write
			barrier.Lock()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			request := DataRequest{AS: scope, ProtocolData: simpleProtocolData("cancelled")}
			go func() { _, err := c.WriteDataContext(ctx, request); done <- err }()
			err, returned := contextResult(done)
			barrier.Unlock()
			if !returned {
				<-done
				t.Fatal("context did not cancel scope admission")
			}
			requireDataWriteError(t, err, DataNotSent, context.DeadlineExceeded)
			if barrier.waiters.count.Load() != 0 {
				t.Fatal("cancelled scope admission retained a waiter")
			}
			if _, err := c.WriteData(request); err != nil {
				t.Fatal(err)
			}
			if capture.submissions() != 1 {
				t.Fatal("cancelled scope send submitted after its barrier was released")
			}
		})
	}
}

func contextResult(done <-chan error) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-time.After(time.Second):
		return nil, false
	}
}

func TestWriteDataContextCancelsBehindEverySender(t *testing.T) {
	for _, kind := range []string{"library control", "application control", "deadline DATA"} {
		t.Run(kind, func(t *testing.T) {
			c, capture := newDataWriteAssociation(t, 1)
			c.signalWriter = nil
			transport := newFullSendBuffer()
			c.controlTransport = transport
			c.cfg.ControlWriteTimeout = time.Minute
			started, release := transport.waiting, transport.space
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			request := allocationRequest(128)
			if kind != "library control" {
				setContextTestWriteDeadline(c, time.Now().Add(time.Minute))
			}
			first := make(chan error, 1)
			switch kind {
			case "deadline DATA":
				c.dataWriter = nil
				go func() { _, err := c.WriteData(request); first <- err }()
			case "application control":
				go func() { _, err := c.WriteSignal(heartbeatAckForTest()); first <- err }()
			default:
				go func() { _, err := c.writeControl(heartbeatAckForTest()); first <- err }()
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				unblock()
				<-first
				t.Fatal("first sender did not park")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.WriteDataContext(ctx, request); done <- err }()
			err, returned := contextResult(done)
			if !returned {
				unblock()
				<-first
				<-done
				t.Fatal("context send queued behind the transport")
			}
			requireDataWriteError(t, err, DataNotSent, context.DeadlineExceeded)
			if c.sendMu.waiters.count.Load() != 0 {
				t.Fatal("cancelled send left a socket waiter")
			}
			unblock()
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			if kind == "deadline DATA" {
				// Returning the parked call is a transport fence. Only that
				// original DATA may have entered either transport attempt.
				if len(transport.waitedFrames()) != 1 {
					t.Fatal("cancelled DATA was submitted late")
				}
				c.dataWriter = capture.write
			}
			if _, err := c.WriteData(request); err != nil {
				t.Fatal(err)
			}
			if capture.submissions() != 1 {
				t.Fatal("cancelled send submitted after the other writer resumed")
			}
		})
	}
}

type exclusiveSendTransport struct {
	active      atomic.Int32
	overlaps    atomic.Int32
	submissions atomic.Int32
}

func (s *exclusiveSendTransport) SendMsg(b []byte, _ sctp.SendOptions) (int, error) {
	if s.active.Add(1) != 1 {
		s.overlaps.Add(1)
	}
	runtime.Gosched()
	s.submissions.Add(1)
	s.active.Add(-1)
	return len(b), nil
}

func (s *exclusiveSendTransport) SendMsgContext(ctx context.Context, b []byte, opts sctp.SendOptions) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.SendMsg(b, opts)
}

func TestSocketGateSerializesEverySubmission(t *testing.T) {
	c, _ := newDataWriteAssociation(t, 1)
	c.dataWriter, c.signalWriter = nil, nil
	transport := &exclusiveSendTransport{}
	c.controlTransport = transport
	var writers sync.WaitGroup
	for range 64 {
		for _, sender := range []func() error{
			func() error { _, err := c.WriteData(allocationRequest(128)); return err },
			func() error { _, err := c.WriteDataContext(t.Context(), allocationRequest(128)); return err },
			func() error { _, err := c.WriteSignal(allocationDataMessage(128, nil)); return err },
			func() error { _, err := c.writeDistributedSignal(allocationDataMessage(128, nil)); return err },
			func() error { _, err := c.WriteSignal(heartbeatAckForTest()); return err },
			func() error { _, err := c.writeControl(heartbeatAckForTest()); return err },
		} {
			writers.Go(func() {
				if err := sender(); err != nil {
					t.Errorf("send: %v", err)
				}
			})
		}
	}
	writers.Wait()
	if transport.overlaps.Load() != 0 || transport.submissions.Load() != 64*6 {
		t.Fatalf("overlaps=%d submissions=%d", transport.overlaps.Load(), transport.submissions.Load())
	}
}

func TestTransportReleaseWakesSocketGateHolder(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "Close", true: "Abort"}[abort], func(t *testing.T) {
			c, _ := newDataWriteAssociation(t, 1)
			c.signalWriter = nil
			transport := newFullSendBuffer()
			c.controlTransport = transport
			c.transportCloser, c.transportAborter = transport.close, transport.close
			c.cfg.ControlWriteTimeout = time.Minute
			done := make(chan error, 1)
			go func() { _, err := c.writeControl(heartbeatAckForTest()); done <- err }()
			select {
			case <-transport.waiting:
			case <-time.After(time.Second):
				t.Fatal("control did not park")
			}
			released := make(chan error, 1)
			go func() {
				if abort {
					released <- c.Abort()
				} else {
					released <- c.Close()
				}
			}()
			if err, ok := contextResult(released); !ok || err != nil {
				_ = transport.close()
				t.Fatalf("release waited for send gate: %v", err)
			}
			if _, ok := contextResult(done); !ok {
				t.Fatal("release did not wake parked sender")
			}
		})
	}
}

func TestContextMethodsPreserveLegacyFrames(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	request := allocationRequest(128)
	request.Stream, request.CorrelationIDSet, request.CorrelationID = 3, true, 163
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := c.WriteData(request); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteDataContext(ctx, request); err != nil {
		t.Fatal(err)
	}
	frames := capture.messages(t)
	first, _ := frames[0].MarshalBinary()
	second, _ := frames[1].MarshalBinary()
	if string(first) != string(second) || capture.streams[0] != capture.streams[1] {
		t.Fatal("context changed DATA or stream")
	}
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	e, _, captures := newASPTransferFixture(t, config)
	mtp := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, []byte("same")), CorrelationIDSet: true, CorrelationID: 163}
	old, err := e.MTPTransfer(mtp)
	if err != nil {
		t.Fatal(err)
	}
	current, err := e.MTPTransferContext(ctx, mtp)
	if err != nil || current.UserDataOctets != old.UserDataOctets || len(current.SuccessfulPaths) != len(old.SuccessfulPaths) {
		t.Fatalf("context transfer: %#v, %v", current, err)
	}
	if captures["sg-a/sgp-a1"].count() != 2 || captures["sg-b/sgp-b1"].count() != 0 {
		t.Fatal("context changed target selection")
	}
	data, _ := captures["sg-a/sgp-a1"].lastData(t)
	if data.CorrelationID == nil || data.CorrelationID.CorrelationID() != 163 {
		t.Fatal("context lost correlation id")
	}
}

func TestMTPTransferContextCancelsActivationBarrier(t *testing.T) {
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	e, associations, captures := newASPTransferFixture(t, config)
	c := associations["sg-a/sgp-a1"]
	c.aspTransferMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, []byte("cancelled"))}
	done := make(chan error, 1)
	go func() { _, err := e.MTPTransferContext(ctx, request); done <- err }()
	err, returned := contextResult(done)
	c.aspTransferMu.Unlock()
	if !returned {
		<-done
		t.Fatal("context did not cancel activation admission")
	}
	requireDataWriteError(t, err, DataNotSent, context.DeadlineExceeded)
	if c.aspTransferMu.waiters.count.Load() != 0 {
		t.Fatal("cancelled transfer retained an activation waiter")
	}
	if _, err := e.MTPTransfer(request); err != nil {
		t.Fatal(err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 || captures["sg-b/sgp-b1"].count() != 0 {
		t.Fatal("cancelled transfer submitted late or retried elsewhere")
	}
}

type contextSendBuffer struct {
	association *Association
	failure     error
	refused     atomic.Bool
	attempt     chan struct{}
	changed     chan struct{}
	accepted    atomic.Int32
}

func newContextSendBuffer(c *Association) *contextSendBuffer {
	return &contextSendBuffer{association: c, attempt: make(chan struct{}, 1), changed: make(chan struct{}, 1)}
}

func (b *contextSendBuffer) SendMsg(frame []byte, opts sctp.SendOptions) (int, error) {
	if b.failure != nil {
		return 0, b.failure
	}
	if b.refused.Load() {
		if !opts.NoWait {
			return 0, errors.New("blocking send needs SendMsgContext")
		}
		select {
		case b.attempt <- struct{}{}:
		default:
		}
		return 0, syscall.EAGAIN
	}
	b.accepted.Add(1)
	return len(frame), nil
}

func (b *contextSendBuffer) SendMsgContext(ctx context.Context, frame []byte, opts sctp.SendOptions) (int, error) {
	if b.failure != nil {
		return 0, b.failure
	}
	if opts.NoWait {
		return b.SendMsg(frame, opts)
	}
	select {
	case b.attempt <- struct{}{}:
	default:
	}
	for b.refused.Load() {
		var deadline <-chan time.Time
		var timer *time.Timer
		if d := b.association.applicationWriteDeadline.Load(); d != nil {
			if !time.Now().Before(*d) {
				return 0, os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(time.Until(*d))
			deadline = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return 0, ctx.Err()
		case <-b.association.done:
			if timer != nil {
				timer.Stop()
			}
			return 0, net.ErrClosed
		case <-deadline:
			return 0, os.ErrDeadlineExceeded
		case <-b.changed:
			if timer != nil {
				timer.Stop()
			}
		}
	}
	b.accepted.Add(1)
	return len(frame), nil
}

func TestWriteDataContextCancelsBufferWait(t *testing.T) {
	c, _ := newDataWriteAssociation(t, 1)
	c.dataWriter = nil
	b := newContextSendBuffer(c)
	b.refused.Store(true)
	c.controlTransport = b
	setContextTestWriteDeadline(c, time.Now().Add(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	request := allocationRequest(128)
	go func() { _, err := c.WriteDataContext(ctx, request); done <- err }()
	select {
	case <-b.attempt:
	case <-time.After(time.Second):
		t.Fatal("context send did not try the full buffer")
	}
	cancel()
	err, returned := contextResult(done)
	if !returned {
		t.Fatal("context did not cancel buffer wait")
	}
	requireDataWriteError(t, err, DataNotSent, context.Canceled)
	b.refused.Store(false)
	if _, err := c.WriteData(request); err != nil {
		t.Fatal(err)
	}
	if b.accepted.Load() != 1 {
		t.Fatal("cancelled buffer waiter submitted after space became available")
	}
}

func TestWriteDataContextBufferWaitEndsOnRelease(t *testing.T) {
	c, _ := newDataWriteAssociation(t, 1)
	c.dataWriter = nil
	b := newContextSendBuffer(c)
	b.refused.Store(true)
	c.controlTransport = b
	setContextTestWriteDeadline(c, time.Now().Add(time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.WriteDataContext(ctx, allocationRequest(128)); done <- err }()
	select {
	case <-b.attempt:
	case <-time.After(time.Second):
		t.Fatal("send did not reach full buffer")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	err, returned := contextResult(done)
	if !returned {
		t.Fatal("association release did not end buffer wait")
	}
	requireDataWriteError(t, err, DataNotSent, ErrAssociationClosed)
	if !c.sendMu.TryLock() {
		t.Fatal("buffer waiter retained socket gate after release")
	}
	c.sendMu.Unlock()
	b.refused.Store(false)
	if b.accepted.Load() != 0 {
		t.Fatal("released waiter submitted DATA")
	}
}

func TestWriteDataContextBufferOutcomes(t *testing.T) {
	for _, variant := range []struct {
		name     string
		deadline time.Time
		cause    error
	}{
		{"no deadline", time.Time{}, syscall.EAGAIN},
		{"expired deadline", time.Now().Add(-time.Second), os.ErrDeadlineExceeded},
		{"deadline while waiting", time.Now().Add(20 * time.Millisecond), os.ErrDeadlineExceeded},
	} {
		t.Run(variant.name, func(t *testing.T) {
			c, _ := newDataWriteAssociation(t, 1)
			c.dataWriter = nil
			b := newContextSendBuffer(c)
			b.refused.Store(true)
			c.controlTransport = b
			if !variant.deadline.IsZero() {
				setContextTestWriteDeadline(c, variant.deadline)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := c.WriteDataContext(ctx, allocationRequest(128))
			requireDataWriteError(t, err, DataNotSent, variant.cause)
			if b.accepted.Load() != 0 {
				t.Fatal("refused DATA was accepted")
			}
		})
	}
}

func TestWriteDataContextObservesDeadlineChanges(t *testing.T) {
	for _, change := range []string{"remove", "expire", "far future"} {
		t.Run(change, func(t *testing.T) {
			c, _ := newDataWriteAssociation(t, 1)
			setContextTestWriteDeadline(c, time.Now().Add(time.Minute))
			b := newContextSendBuffer(c)
			b.refused.Store(true)
			c.dataWriter = nil
			c.controlTransport = b
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.WriteDataContext(ctx, allocationRequest(128)); done <- err }()
			select {
			case <-b.attempt:
			case <-time.After(time.Second):
				t.Fatal("send did not reach full buffer")
			}
			switch change {
			case "remove":
				setContextTestWriteDeadline(c, time.Time{})
			case "expire":
				setContextTestWriteDeadline(c, time.Now().Add(-time.Second))
			case "far future":
				setContextTestWriteDeadline(c, time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC))
			}
			if change != "expire" {
				b.refused.Store(false)
				b.changed <- struct{}{}
			}
			err, returned := contextResult(done)
			if !returned {
				t.Fatal("changed deadline did not release buffer wait")
			}
			if change == "expire" {
				requireDataWriteError(t, err, DataNotSent, os.ErrDeadlineExceeded)
			} else if err != nil || b.accepted.Load() != 1 {
				t.Fatalf("changed deadline: %v, accepted=%d", err, b.accepted.Load())
			}
		})
	}
}

func TestWriteDataContextTransportErrorsQueueNothing(t *testing.T) {
	for _, cause := range []error{io.ErrUnexpectedEOF, context.Canceled, syscall.EIO} {
		t.Run(cause.Error(), func(t *testing.T) {
			c, _ := newDataWriteAssociation(t, 1)
			c.dataWriter = nil
			b := newContextSendBuffer(c)
			b.failure = cause
			c.controlTransport = b
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			n, err := c.WriteDataContext(ctx, allocationRequest(128))
			if n != 0 {
				t.Fatalf("failed send returned %d octets", n)
			}
			requireDataWriteError(t, err, DataNotSent, cause)
		})
	}
}

func TestWriteDataContextSubmissionOutcomes(t *testing.T) {
	for _, variant := range []struct {
		name    string
		written int
		err     error
		outcome DataSendOutcome
	}{
		{"unknown failure", 0, io.ErrUnexpectedEOF, DataSendIndeterminate},
		{"cancelled transport", 0, context.Canceled, DataSendIndeterminate},
		{"whole refusal", 0, syscall.EAGAIN, DataNotSent},
		{"partial refusal", 1, syscall.EAGAIN, DataSendIndeterminate},
		{"short write", 1, nil, DataSendIndeterminate},
	} {
		t.Run(variant.name, func(t *testing.T) {
			c, _ := newDataWriteAssociation(t, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.dataWriter = func(_ []byte, _ *sctp.SndInfo) (int, error) {
				cancel()
				return variant.written, variant.err
			}
			_, err := c.WriteDataContext(ctx, allocationRequest(128))
			requireDataWriteError(t, err, variant.outcome, context.Canceled)
			cause := variant.err
			if cause == nil {
				cause = ErrPartialDataWrite
			}
			if !errors.Is(err, cause) {
				t.Fatalf("error lost transport cause: %v", err)
			}
		})
	}
	// Cancellation cannot retract an accepted message.
	c, _ := newDataWriteAssociation(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.dataWriter = func(b []byte, _ *sctp.SndInfo) (int, error) { cancel(); return len(b), nil }
	if n, err := c.WriteDataContext(ctx, allocationRequest(128)); n != 128 || err != nil {
		t.Fatalf("accepted message = %d, %v", n, err)
	}
}

func TestMTPTransferContextKeepsCompletedBroadcastPaths(t *testing.T) {
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	e, associations, captures := newASPTransferFixture(t, config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := captures["sg-a/sgp-a1"]
	associations["sg-a/sgp-a1"].dataWriter = func(b []byte, info *sctp.SndInfo) (int, error) {
		cancel()
		return first.write(b, info)
	}
	result, err := e.MTPTransferContext(ctx, MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, []byte("broadcast"))})
	var transferErr *MTPTransferError
	if !errors.As(err, &transferErr) || len(transferErr.Failures) != 1 || len(result.SuccessfulPaths) != 1 {
		t.Fatalf("partial broadcast = %#v, %v", result, err)
	}
	requireDataWriteError(t, transferErr.Failures[0].Err, DataNotSent, context.Canceled)
	if result.UserDataOctets != len("broadcast") || first.count() != 1 || captures["sg-b/sgp-b1"].count() != 0 {
		t.Fatal("broadcast cancellation lost its successful path or submitted another")
	}
}

func TestWriteDataContextWaitersDoNotLeak(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	baseline := runtime.NumGoroutine()
	c.sendMu.Lock()
	const writers = 128
	var group sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	for range writers {
		group.Go(func() {
			_, err := c.WriteDataContext(ctx, allocationRequest(128))
			if !errors.Is(err, context.Canceled) {
				t.Errorf("cancelled writer: %v", err)
			}
		})
	}
	deadline := time.Now().Add(time.Second)
	for c.sendMu.waiters.count.Load() != writers && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	parked := c.sendMu.waiters.count.Load()
	cancel()
	group.Wait()
	retained := c.sendMu.waiters.count.Load()
	c.sendMu.Unlock()
	if parked != writers || retained != 0 {
		t.Fatalf("socket waiters: parked=%d retained=%d", parked, retained)
	}
	if _, err := c.WriteData(allocationRequest(128)); err != nil {
		t.Fatal(err)
	}
	if capture.submissions() != 1 {
		t.Fatal("cancelled waiters submitted late")
	}
	deadline = time.Now().Add(time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := runtime.NumGoroutine(); got > baseline {
		t.Fatalf("goroutines grew from %d to %d", baseline, got)
	}
}

func TestWriteDataContextAllocations(t *testing.T) {
	for _, size := range []int{128, 4096} {
		t.Run(payloadName(size), func(t *testing.T) {
			c, request := newSendAllocationAssociation(t), allocationRequest(size)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			old := testing.AllocsPerRun(1000, func() {
				if _, err := c.WriteData(request); err != nil {
					t.Fatal(err)
				}
			})
			withContext := testing.AllocsPerRun(1000, func() {
				if _, err := c.WriteDataContext(ctx, request); err != nil {
					t.Fatal(err)
				}
			})
			if withContext != old {
				t.Fatalf("context allocations = %v, WriteData = %v", withContext, old)
			}
		})
	}
}

func TestMTPTransferContextCancelledBeforeAdmission(t *testing.T) {
	e, _, captures := newASPTransferFixture(t, validASPConfig())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := e.MTPTransferContext(ctx, MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, []byte("cancelled")),
	})
	requireDataWriteError(t, err, DataNotSent, context.Canceled)
	if len(result.SuccessfulPaths) != 0 {
		t.Fatal("cancelled transfer reported success")
	}
	for _, capture := range captures {
		if capture.count() != 0 {
			t.Fatal("cancelled transfer reached a transport")
		}
	}
}

func TestMTPTransferContextCancelsFlowWait(t *testing.T) {
	e, _, captures := newASPTransferFixture(t, validASPConfig())
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, []byte("cancelled"))}
	sequence, err := e.aspRoutes.lockTransferSequence(request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := e.MTPTransferContext(ctx, request); done <- err }()
	select {
	case err := <-done:
		requireDataWriteError(t, err, DataNotSent, context.DeadlineExceeded)
	case <-time.After(time.Second):
		sequence.release()
		<-done
		t.Fatal("context did not cancel the flow wait")
	}
	sequence.release()
	// A later synchronous send is also a fence: a cancelled waiter must have
	// relinquished its flow reference and cannot submit after it returns.
	if _, err := e.MTPTransfer(request); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, capture := range captures {
		count += capture.count()
	}
	if count != 1 {
		t.Fatalf("submissions after flow release = %d, want only the later send", count)
	}
	e.aspRoutes.transferSequenceMu.Lock()
	retained := len(e.aspRoutes.transferSequences)
	e.aspRoutes.transferSequenceMu.Unlock()
	if retained != 0 {
		t.Fatalf("retained flow locks = %d", retained)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("test context did not expire")
	}
}
