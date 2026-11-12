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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// releaseCauseSender faults only the write syscall. The real reader, control
// worker, state machine and teardown still determine the application's cause.
type releaseCauseSender struct {
	conn    *sctp.Conn
	ctx     context.Context
	armed   atomic.Bool
	err     error
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	unblock sync.Once
	// A later response can remain inside a transport call while the reader
	// observes the peer end. This checks teardown's independence from monitor.
	blockLater bool
	calls      atomic.Int32
	blocked    chan struct{}
	blockOnce  sync.Once
	done       <-chan struct{}
}

func (s *releaseCauseSender) SendMsg(b []byte, opts sctp.SendOptions) (int, error) {
	if !s.armed.Load() {
		return s.conn.SendMsg(b, opts)
	}
	if s.calls.Add(1) > 1 && s.blockLater {
		s.blockOnce.Do(func() { close(s.blocked) })
		select {
		case <-s.done:
			return 0, net.ErrClosed
		case <-s.ctx.Done():
			return 0, s.ctx.Err()
		}
	}
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return 0, &net.OpError{Op: "write", Net: "sctp", Err: s.err}
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	}
}

func (s *releaseCauseSender) SendMsgContext(ctx context.Context, b []byte, opts sctp.SendOptions) (int, error) {
	if !s.armed.Load() {
		return s.conn.SendMsgContext(ctx, b, opts)
	}
	return s.SendMsg(b, opts)
}

func (s *releaseCauseSender) releaseWrite() { s.unblock.Do(func() { close(s.release) }) }

func waitForReleaseCauseStep(t *testing.T, done <-chan struct{}, step string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", step)
	}
}

func newReleaseCausePeer(t *testing.T, port int, writeErr error) (*Association, *sctp.Conn, *releaseCauseSender, <-chan struct{}, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	addr, err := sctp.ResolveAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	watcher := &restartWatcher{}
	listener, err := listenSCTP("sctp", addr, socketBuffers{}, nil, false, watcher)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	type accepted struct {
		conn *sctp.Conn
		err  error
	}
	ready := make(chan accepted, 1)
	go func() {
		conn, err := listener.AcceptSCTP()
		ready <- accepted{conn, err}
	}()
	peer, err := sctp.Dial(ctx, "sctp", nil, addr)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	var result accepted
	select {
	case result = <-ready:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("accept did not finish")
	}
	if result.err != nil {
		cancel()
		t.Fatal(result.err)
	}
	cfg := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1, 2})
	cfg.ControlWriteTimeout = time.Minute
	conn := newAssociation(RoleSGP, cfg)
	conn.sctpConn = result.conn
	watcher.setRoute(func(sctp.AssocID) *Association { return conn })
	conn.ManagementIndications()
	t.Cleanup(func() { _ = conn.Close() })
	t.Cleanup(cancel)
	requireSubscribedAssociationEvents(t, result.conn, "faulted control writer's reader")
	sender := &releaseCauseSender{
		conn: result.conn, ctx: ctx, err: writeErr,
		entered: make(chan struct{}), release: make(chan struct{}),
		blocked: make(chan struct{}), done: conn.Done(),
	}
	conn.controlTransport = sender
	// Observe the real worker's return, after its end-cause decision. A
	// successful enqueue alone would not prove that the write noticed first.
	conn.notificationOnce.Do(func() {})
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); conn.writeNotifications() }()
	go conn.monitor(ctx)

	// A round trip proves that monitor, dispatcher, reader and control worker
	// are all running before the injected failure is armed.
	beat, err := messages.NewHeartbeat(params.NewHeartbeatData([]byte("reader-ready"))).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sendWithInfo(peer, beat, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	raw, _, err := peer.ReadMsg(DefaultReadBufferSize)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := messages.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := msg.(*messages.HeartbeatAck); !ok {
		t.Fatalf("readiness response is %T, want HeartbeatAck", msg)
	}
	if err := peer.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	return conn, peer, sender, workerDone, cancel
}

func TestPeerReleaseCauseSurvivesAControlWriteFailure(t *testing.T) {
	port := 5930
	for _, errno := range []syscall.Errno{syscall.ECONNRESET, syscall.EPIPE, syscall.ESHUTDOWN} {
		for _, graceful := range []bool{false, true} {
			for _, readerFirst := range []bool{false, true} {
				port++
				t.Run(fmt.Sprintf("errno_%d/graceful_%t/reader_first_%t", errno, graceful, readerFirst), func(t *testing.T) {
					conn, peer, sender, workerDone, _ := newReleaseCausePeer(t, port, errno)
					sender.armed.Store(true)
					if err := conn.writeMandatoryControls([]messages.M3UA{heartbeatAckForTest()}, false, false); err != nil {
						t.Fatal(err)
					}
					waitForReleaseCauseStep(t, sender.entered, "faulted write")
					if !readerFirst {
						sender.releaseWrite()
						waitForReleaseCauseStep(t, workerDone, "control worker")
						select {
						case <-conn.Done():
							t.Fatalf("write error finalized association before peer release: %v", conn.Err())
						default:
						}
					}
					if graceful {
						if err := peer.Close(); err != nil {
							t.Fatal(err)
						}
					} else if err := peer.Abort(); err != nil {
						t.Fatal(err)
					}
					requireDone(t, conn, "peer release")
					if readerFirst {
						sender.releaseWrite()
						waitForReleaseCauseStep(t, workerDone, "control worker after reader")
					}
					cause := conn.Err()
					if graceful {
						if !errors.Is(cause, io.EOF) {
							t.Fatalf("Err = %v, want the peer's graceful EOF", cause)
						}
					} else if !errors.Is(cause, ErrSCTPNotAlive) || !strings.Contains(cause.Error(), sctp.CauseUserAbort.String()) {
						t.Fatalf("Err = %v, want SCTP_COMM_LOST with User-Initiated Abort", cause)
					}
					if errors.Is(cause, errno) {
						t.Fatalf("Err retained the faulted write errno: %v", cause)
					}
					for {
						select {
						case indication, ok := <-conn.ManagementIndications():
							if !ok {
								t.Fatal("no SCTP release indication")
							}
							if indication.Kind == ManagementSCTPRelease {
								if indication.Cause != cause {
									t.Fatalf("release cause = %v, want Err's %v", indication.Cause, cause)
								}
								return
							}
						case <-time.After(5 * time.Second):
							t.Fatal("SCTP release indication did not arrive")
						}
					}
				})
			}
		}
	}
}

func TestOwnerCancellationEndsAPendingErrorResponse(t *testing.T) {
	conn, peer, sender, _, cancel := newReleaseCausePeer(t, 5940, syscall.ECONNRESET)
	sender.armed.Store(true)
	badVersion, err := messages.NewHeartbeat(nil).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	badVersion[0] = 2
	if _, err := sendWithInfo(peer, badVersion, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); err != nil {
		t.Fatal(err)
	}
	waitForReleaseCauseStep(t, sender.entered, "ERR response write")
	sender.releaseWrite()
	waitForReleaseCauseStep(t, conn.peerRelease.drainSignal(), "pending transport failure")
	cancel()
	requireDone(t, conn, "owner cancellation during ERR response failure")
	if !errors.Is(conn.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want owner cancellation", conn.Err())
	}
}

func TestPeerReleaseCompletesWhileAnErrorResponseIsBlocked(t *testing.T) {
	conn, peer, sender, workerDone, _ := newReleaseCausePeer(t, 5941, syscall.ECONNRESET)
	sender.blockLater = true
	sender.armed.Store(true)
	sender.releaseWrite()
	if err := conn.writeMandatoryControls([]messages.M3UA{heartbeatAckForTest()}, false, false); err != nil {
		t.Fatal(err)
	}
	waitForReleaseCauseStep(t, workerDone, "failed notification worker")
	select {
	case conn.errChan <- NewInvalidRoutingContextError(99):
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not accept the error response request")
	}
	waitForReleaseCauseStep(t, sender.blocked, "blocked ERR response")
	if err := peer.Abort(); err != nil {
		t.Fatal(err)
	}
	requireDone(t, conn, "peer ABORT while monitor's response is blocked")
	if !errors.Is(conn.Err(), ErrSCTPNotAlive) || !strings.Contains(conn.Err().Error(), sctp.CauseUserAbort.String()) {
		t.Fatalf("Err = %v, want the peer's ABORT cause", conn.Err())
	}
}
