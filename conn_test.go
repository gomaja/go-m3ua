// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/google/go-cmp/cmp"

	"github.com/gomaja/go-sctp"
)

// setupConn establishes a client/server M3UA pair over a real SCTP
// association, with BEAT disabled on both ends. Each caller passes a distinct
// port so that tests running in the same binary cannot collide on the
// listening address, and the listener is closed when the test finishes.
// On platforms without SCTP support the calling test is skipped.
func setupConn(t *testing.T, ctx context.Context, port int) (*Conn, *Conn, error) {
	t.Helper()
	return setupConnHB(t, ctx, port,
		&HeartbeatInfo{Enabled: false}, &HeartbeatInfo{Enabled: false})
}

func isSCTPUnsupported(err error) bool {
	return errors.Is(err, sctp.ErrUnsupported) ||
		(err != nil && strings.Contains(err.Error(), "SCTP is unsupported"))
}

func skipIfSCTPUnsupported(t *testing.T, err error) {
	t.Helper()
	if isSCTPUnsupported(err) {
		t.Skipf("skipping socket-backed test: %v", err)
	}
}

// setupConnHB is setupConn with per-end heartbeat configuration, so tests can
// exercise the BEAT/BEAT Ack exchange over a real association.
func setupConnHB(t *testing.T, ctx context.Context, port int, cliHB, srvHB *HeartbeatInfo) (*Conn, *Conn, error) {
	t.Helper()

	// Both buffered, so the accept goroutine below can always finish its send
	// and exit. While they were unbuffered, every setup that ended by any route
	// other than the success case — the error case, or the 10 second timeout —
	// left that goroutine blocked on a send nobody would ever receive, one
	// leaked goroutine per occurrence for the life of the test binary. This
	// package has three tests that assert on goroutine counts, so those leaks
	// did not stay local to the test that produced them.
	var (
		srvConnChan = make(chan *Conn, 1)
		errChan     = make(chan error, 1)
	)

	srvCfg := NewServerConfig(
		srvHB,
		0x22222222,                  // OriginatingPointCode
		0x11111111,                  // DestinationPointCode
		1,                           // AspIdentifier
		params.TrafficModeLoadshare, // TrafficModeType
		0,                           // NetworkAppearance
		0,                           // CorrelationID
		[]uint32{1, 2},              // RoutingContexts
		params.ServiceIndSCCP,       // ServiceIndicator
		0,                           // NetworkIndicator
		0,                           // MessagePriority
		1,                           // SignalingLinkSelection
	)
	// set nil on unnecessary parameters.
	srvCfg.AspIdentifier = nil
	srvCfg.CorrelationID = nil

	// setup SCTP peer on the specified IPs and Port.
	raddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		return nil, nil, err
	}

	listener, err := Listen("m3ua", raddr, NewListenerConfig(srvCfg))
	if err != nil {
		// The sctp package returns this on platforms without SCTP support
		// (e.g. darwin); the socket-backed tests are meaningful only where a
		// real association can be built, so skip rather than fail.
		skipIfSCTPUnsupported(t, err)
		return nil, nil, err
	}
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		srvConn, err := listener.Accept(ctx)
		if err != nil {
			// Returning here is the point: without it the failure path fell
			// through and also sent the nil Conn on srvConnChan. Whichever
			// channel the caller's select happened to take, the other send was
			// left dangling — and if it took srvConnChan first it proceeded
			// with a nil *Conn and panicked on the next method call, reporting
			// a crash rather than the Accept error that actually caused it.
			errChan <- err
			return
		}

		srvConnChan <- srvConn
	}()

	cliCfg := NewClientConfig(
		cliHB,
		0x11111111,                  // OriginatingPointCode
		0x22222222,                  // DestinationPointCode
		1,                           // AspIdentifier
		params.TrafficModeLoadshare, // TrafficModeType
		0,                           // NetworkAppearance
		0,                           // CorrelationID
		[]uint32{1, 2},              // RoutingContexts
		params.ServiceIndSCCP,       // ServiceIndicator
		0,                           // NetworkIndicator
		0,                           // MessagePriority
		1,                           // SignalingLinkSelection
	)
	// set nil on unnecessary parameters.
	cliCfg.CorrelationID = nil

	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, nil, err
	}

	cliConn, err := Dial(ctx, "m3ua", laddr, raddr, cliCfg)
	if err != nil {
		return nil, nil, err
	}

	select {
	case srvConn := <-srvConnChan:
		// Production must buffer exactly one BEAT Ack token (see
		// notifyBeatAck); the socket tests are the only place Dial- and
		// Accept-built Conns are visible, so pin the capacity here to guard
		// the constructors against a silent regression to unbuffered.
		for name, c := range map[string]*Conn{"client": cliConn, "server": srvConn} {
			if cap(c.beatAckChan) != 1 {
				t.Errorf("%s beatAckChan capacity = %d, want 1 (an Ack arriving before heartbeat() parks would be lost)",
					name, cap(c.beatAckChan))
			}
		}
		// Two Routing Contexts are coordinated, so each DATA has to name the
		// one identifying its traffic flow (RFC 4666 Section 3.3.1). These
		// tests are not about distribution across Application Servers, so both
		// ends pick the same one.
		for _, c := range []*Conn{cliConn, srvConn} {
			if err := c.SelectRoutingContext(1); err != nil {
				return nil, nil, err
			}
		}

		return cliConn, srvConn, nil
	case err := <-errChan:
		return nil, nil, err
	case <-time.After(10 * time.Second):
		return nil, nil, errors.New("timeout")
	}
}

// TestDuplicateAspUpIsAcked reproduces the interop failure where a peer whose
// view of the ASP state has diverged keeps retransmitting ASP Up: withholding
// the ASP Up Ack leaves the peer looping on T(ack) forever.
//
// RFC 4666 Section 4.3.4.1 requires an ASP Up Ack in response to every ASP Up,
// including on an already-established association, so the exchange converges.
func TestDuplicateAspUpIsAcked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 2906)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	// The association is up; resend ASP Up as a desynchronised peer would. The
	// client drops to ASP-DOWN first so that the ASP Up Ack it expects back
	// drives it forward again, mirroring a peer that has lost its state.
	if err := cliConn.handleStateUpdate(StateAspDown); err != nil {
		t.Fatal(err)
	}

	// RFC 4666 Section 4.3.4.1: the Ack is mandatory in every state. Without
	// it the client never leaves ASP-DOWN and would retransmit ASP Up until
	// T(ack) expires, indefinitely.
	// Each phase gets its own budget: a context's Done channel stays closed
	// once it expires, unlike a one-shot time.After that a single receive
	// drains for good.
	ackCtx, cancelAck := context.WithTimeout(ctx, 20*time.Second)
	defer cancelAck()
	for cliConn.State() == StateAspDown {
		select {
		case <-ackCtx.Done():
			t.Fatal("client stuck in AspDown: no ASP Up Ack received for the duplicate ASP Up")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Both ends must converge back to ASP-ACTIVE so traffic resumes.
	convCtx, cancelConv := context.WithTimeout(ctx, 20*time.Second)
	defer cancelConv()
	for cliConn.State() != StateAspActive || srvConn.State() != StateAspActive {
		select {
		case <-convCtx.Done():
			t.Fatalf("states = client %v / server %v, want both %v after duplicate ASP Up",
				cliConn.State(), srvConn.State(), StateAspActive)
		case <-time.After(20 * time.Millisecond):
		}
	}

	msg := []byte{0xde, 0xad, 0xbe, 0xef}
	if _, err := cliConn.Write(msg); err != nil {
		t.Fatalf("write after duplicate ASP Up: %v", err)
	}

	buf := make([]byte, 1024)
	n, err := srvConn.Read(buf)
	if err != nil {
		t.Fatalf("read after duplicate ASP Up: %v", err)
	}
	if diff := cmp.Diff(buf[:n], msg); diff != "" {
		t.Error(diff)
	}
}

// TestHeartbeatKeepsAssociationAlive runs the real heartbeat() loop on both
// ends of a live association — the only test that does — so the BEAT / BEAT
// Ack round trip, the Ack validation in handleHeartbeatAck, and the token
// handoff through beatAckChan are all exercised end to end. Several T(beat)
// intervals must elapse with both ends still ASP-ACTIVE and passing data: a
// lost or rejected Ack would fire ErrHeartbeatExpired and tear the healthy
// association down.
func TestHeartbeatKeepsAssociationAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hb := func() *HeartbeatInfo { return NewHeartbeatInfo(50*time.Millisecond, 2*time.Second, nil) }
	cliConn, srvConn, err := setupConnHB(t, ctx, 2907, hb(), hb())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	// Roughly ten beat rounds per end.
	time.Sleep(500 * time.Millisecond)

	if got := cliConn.State(); got != StateAspActive {
		t.Errorf("client state = %v, want %v after heartbeat soak", got, StateAspActive)
	}
	if got := srvConn.State(); got != StateAspActive {
		t.Errorf("server state = %v, want %v after heartbeat soak", got, StateAspActive)
	}

	msg := []byte{0xde, 0xad, 0xbe, 0xef}
	if _, err := cliConn.Write(msg); err != nil {
		t.Fatalf("write after heartbeat soak: %v", err)
	}
	buf := make([]byte, 1024)
	n, err := srvConn.Read(buf)
	if err != nil {
		t.Fatalf("read after heartbeat soak: %v", err)
	}
	if diff := cmp.Diff(buf[:n], msg); diff != "" {
		t.Error(diff)
	}
}

func TestReadWrite(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 2905)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	msg := []byte{0xde, 0xad, 0xbe, 0xef}
	buf := make([]byte, 1024)

	t.Run("client-write", func(t *testing.T) {
		if _, err := cliConn.Write(msg); err != nil {
			t.Fatal(err)
		}

		n, err := srvConn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(buf[:n], msg); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("server-write", func(t *testing.T) {
		if _, err := srvConn.Write(msg); err != nil {
			t.Fatal(err)
		}

		n, err := cliConn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(buf[:n], msg); diff != "" {
			t.Error(diff)
		}
	})
}
