// Copyright 2026 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-sctp"
)

func newPeerReleaseTestConn(t *testing.T) *Association {
	t.Helper()
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	installReleaseSeams(conn, nil)
	conn.signalWriter = nil
	sender := newFullSendBuffer()
	sender.firstErr = &net.OpError{Op: "write", Net: "sctp", Err: syscall.ECONNRESET}
	conn.controlTransport = sender
	conn.peerRelease.startReader()
	return conn
}

func pendingPeerRelease(t *testing.T) (*Association, error) {
	t.Helper()
	conn := newPeerReleaseTestConn(t)
	_, err := conn.writeSignal(heartbeatAckForTest(), false, libraryWrite)
	if !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("control write = %v, want ECONNRESET", err)
	}
	return conn, err
}

func TestPeerReleaseRetainsAReaderCausePublishedBeforeTheWriter(t *testing.T) {
	conn := newPeerReleaseTestConn(t)
	peerCause := fmt.Errorf("%w: received synthetic ABORT", ErrSCTPNotAlive)
	conn.peerRelease.finishedRead(peerCause)
	_, err := conn.writeSignal(heartbeatAckForTest(), false, libraryWrite)
	if !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal(err)
	}
	if err := conn.closeWith(err); err != nil {
		t.Fatal(err)
	}
	if conn.Err() != peerCause {
		t.Fatalf("Err = %v, want the already-published reader cause %v", conn.Err(), peerCause)
	}
}

func TestPeerReleaseSurvivesAnErrorWrapperThatDropsTheWriteErrno(t *testing.T) {
	conn, _ := pendingPeerRelease(t)
	// Heartbeat reports this category rather than returning its write errno.
	if err := conn.closeWith(ErrFailedToWriteSignal); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.Done():
		t.Fatalf("wrapped write failure finalized Err as %v", conn.Err())
	default:
	}
	conn.peerRelease.finishedRead(io.EOF)
	if err := conn.closeWith(ErrFailedToWriteSignal); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(conn.Err(), io.EOF) {
		t.Fatalf("Err = %v, want the peer's EOF", conn.Err())
	}
}

func TestPendingPeerReleaseRejectsMandatoryControls(t *testing.T) {
	conn, _ := pendingPeerRelease(t)
	conn.notificationQueue = make(chan mandatoryControl, 1)
	conn.notificationQueue <- mandatoryControl{messages: []messages.M3UA{heartbeatAckForTest()}}
	conn.notificationOnce.Do(func() {})
	for _, wait := range []bool{false, true} {
		err := conn.writeMandatoryControls([]messages.M3UA{heartbeatAckForTest()}, false, wait)
		if !errors.Is(err, syscall.ECONNRESET) || errors.Is(err, ErrNotificationQueueFull) {
			t.Fatalf("later control write = %v, want the pending transport failure", err)
		}
	}
	select {
	case <-conn.Done():
		t.Fatalf("later control writes replaced the peer cause with %v", conn.Err())
	default:
	}
}

func TestPendingPeerReleaseReleasesQueuedControlWaiters(t *testing.T) {
	conn := newPeerReleaseTestConn(t)
	conn.notificationQueue = make(chan mandatoryControl, 1)
	conn.notificationOnce.Do(func() {})
	result := make(chan error, 1)
	go func() {
		result <- conn.writeMandatoryControls([]messages.M3UA{heartbeatAckForTest()}, false, true)
	}()
	select {
	case <-conn.notificationQueue:
	case <-time.After(5 * time.Second):
		t.Fatal("control was not queued")
	}
	if _, err := conn.writeSignal(heartbeatAckForTest(), false, libraryWrite); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("queued waiter = %v, want transport failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued control waiter outlived the failed writer")
	}
}

func TestPendingPeerReleaseUnblocksSCTPNotificationHandoffs(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart_%t", restart), func(t *testing.T) {
			conn, _ := pendingPeerRelease(t)
			conn.inboundChan = make(chan inbound)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if restart {
					conn.enqueueSCTPRestart()
				} else {
					conn.enqueueSCTPPeerAddrChange(&sctp.PeerAddrChange{})
				}
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("SCTP notification waited on an unwinding dispatcher")
			}
			if conn.Err() != nil {
				t.Fatalf("handoff supplied a spurious end cause: %v", conn.Err())
			}
		})
	}
}

func TestLocalReleaseWinsOverAPendingPeerCause(t *testing.T) {
	for _, aborted := range []bool{false, true} {
		t.Run(fmt.Sprintf("abort_%t", aborted), func(t *testing.T) {
			conn, _ := pendingPeerRelease(t)
			want := ErrAssociationClosed
			var err error
			if aborted {
				want = ErrAssociationAborted
				err = conn.Abort()
			} else {
				err = conn.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if conn.Err() != want {
				t.Fatalf("Err = %v, want explicit local release %v", conn.Err(), want)
			}
			conn.peerRelease.finishedRead(io.EOF)
			if err := conn.closeWith(io.EOF); err != nil {
				t.Fatal(err)
			}
			if conn.Err() != want {
				t.Fatalf("late reader changed local cause to %v", conn.Err())
			}
		})
	}
}

func TestPeerReadCausePreservesLossCategoryAndErrno(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ECONNRESET, syscall.EPIPE, syscall.ENOTCONN, syscall.ECONNABORTED, syscall.ETIMEDOUT, syscall.ESHUTDOWN} {
		err := peerReadCause(&net.OpError{Op: "read", Net: "sctp", Err: errno})
		if !errors.Is(err, ErrSCTPNotAlive) || !errors.Is(err, errno) {
			t.Errorf("read %v became %v, want both the loss category and errno", errno, err)
		}
	}
	for _, cause := range []error{io.EOF, context.Canceled, net.ErrClosed, errors.New("malformed notification"), ErrSCTPNotAlive} {
		if got := peerReadCause(cause); got != cause {
			t.Errorf("read cause %v became %v", cause, got)
		}
	}
}

func TestShutdownReturnsReaderCauseAfterAWithdrawalWriteFailure(t *testing.T) {
	conn := newPeerReleaseTestConn(t)
	conn.role = RoleASP
	conn.peerRelease.finishedRead(io.EOF)
	err := conn.ShutdownContext(context.Background())
	if !errors.Is(err, io.EOF) || conn.Err() != io.EOF {
		t.Fatalf("ShutdownContext = %v; Err = %v, want the reader's EOF for both", err, conn.Err())
	}
}

type withdrawalFailureSender struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *withdrawalFailureSender) SendMsg([]byte, sctp.SendOptions) (int, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return 0, &net.OpError{Op: "write", Net: "sctp", Err: syscall.ECONNRESET}
}

func (s *withdrawalFailureSender) SendMsgContext(_ context.Context, b []byte, opts sctp.SendOptions) (int, error) {
	return s.SendMsg(b, opts)
}

func TestShutdownCancellationReleasesAPendingPeerFailure(t *testing.T) {
	conn := newPeerReleaseTestConn(t)
	conn.role = RoleASP
	sender := &withdrawalFailureSender{entered: make(chan struct{}), release: make(chan struct{})}
	conn.controlTransport = sender
	var release sync.Once
	unblock := func() { release.Do(func() { close(sender.release) }) }
	defer unblock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- conn.ShutdownContext(ctx) }()
	select {
	case <-sender.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("withdrawal did not enter the transport")
	}
	cancel()
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !errors.Is(conn.Err(), context.Canceled) {
			t.Fatalf("ShutdownContext = %v; Err = %v, want caller cancellation to release SCTP", err, conn.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled withdrawal waited for peer release")
	}
}

func TestFailedWithdrawalJoinsAnAlreadyCommittedTeardown(t *testing.T) {
	conn := newPeerReleaseTestConn(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	conn.transportCloser = func() error {
		close(entered)
		<-release
		return nil
	}
	closed := make(chan struct{})
	go func() { defer close(closed); _ = conn.closeWith(ErrHeartbeatExpired) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("internal teardown did not reach transport release")
	}
	_, writeErr := conn.writeSignal(heartbeatAckForTest(), false, libraryWrite)
	if !errors.Is(writeErr, syscall.ECONNRESET) {
		t.Fatal(writeErr)
	}
	result := make(chan error, 1)
	go func() { result <- conn.finishTermination(context.Background(), writeErr) }()
	select {
	case err := <-result:
		t.Fatalf("withdrawal returned %v while the committed transport release was blocked", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-result:
		if err != ErrHeartbeatExpired || conn.Err() != err {
			t.Fatalf("withdrawal = %v; Err = %v, want the committed heartbeat failure", err, conn.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("withdrawal did not join the completed teardown")
	}
	<-closed
}

func TestLocalShutdownDuringPendingPeerRelease(t *testing.T) {
	conn, _ := pendingPeerRelease(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- conn.ShutdownContext(ctx) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("SGP ShutdownContext = %v, want its immediate Close", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SGP ShutdownContext waited for a peer cause instead of closing locally")
	}
	if conn.Err() != ErrAssociationClosed {
		t.Fatalf("Err = %v, want local Close", conn.Err())
	}
}
