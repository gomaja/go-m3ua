// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Association is documented as satisfying net.Conn, where a read deadline bounds the
// read and nothing else: the error reports Timeout() true and the connection
// stays usable.
//
// This one did neither. Read never consulted a deadline at all; the value was
// pushed down to the SCTP socket, whose only reader is this package's receive
// loop, and that loop's error path closes the association. So the idiomatic
//
//	conn.SetReadDeadline(time.Now().Add(d))
//	n, err := conn.Read(buf)
//
// on an idle association destroyed it. Measured before the fix: a healthy
// ASP-ACTIVE association went to ASP-DOWN with Err() "i/o timeout", and every
// subsequent Read and Write returned ErrNotEstablished. Heartbeats are off
// unless configured, so "idle" is the normal state of a quiet ASP.
func TestReadDeadlineBoundsReadWithoutEndingTheAssociation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3221)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	if err := cliConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	// Push one message through first. A deadline does not interrupt a recvmsg
	// already in flight, so before the fix it only took hold once the receive
	// loop cycled — which is exactly what this does, and is what made the
	// destruction reproducible rather than occasional.
	pd := params.NewProtocolData(
		0x22222222, 0x11111111, params.ServiceIndSCCP, 0, 0, 1, []byte("wake"))
	if _, err := srvConn.WritePDWithRoutingContext(pd, 1); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if _, err := cliConn.ReadPD(); err != nil {
		t.Fatalf("reading the first message: %v", err)
	}

	start := time.Now()
	n, err := cliConn.Read(make([]byte, 32))
	elapsed := time.Since(start)

	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read after the deadline = (%d, %v), want os.ErrDeadlineExceeded", n, err)
	}
	// net.Conn's contract: a read timeout is a net.Error reporting Timeout().
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Errorf("error %v does not report Timeout(); a caller cannot tell it "+
			"apart from a real failure", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Read took %v to report the deadline", elapsed)
	}

	// The association must be untouched. This is the assertion that fails
	// against the old behaviour even if the error above somehow matched.
	if state := cliConn.State(); state != StateASPActive {
		t.Errorf("state = %v after a read deadline expired, want %v; the "+
			"deadline tore down a healthy association", state, StateASPActive)
	}
	select {
	case <-cliConn.Done():
		t.Fatalf("the association was closed by a read deadline: %v", cliConn.Err())
	default:
	}

	// And it must still carry traffic in both directions afterwards.
	if err := cliConn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clearing the deadline: %v", err)
	}
	if _, err := srvConn.WritePDWithRoutingContext(pd, 1); err != nil {
		t.Fatalf("peer write after the deadline: %v", err)
	}
	got, err := cliConn.ReadPD()
	if err != nil {
		t.Fatalf("read after the deadline was cleared: %v", err)
	}
	if string(got.Data) != "wake" {
		t.Errorf("payload = %q, want %q", got.Data, "wake")
	}
}

// A deadline already in the past reports immediately rather than waiting, and a
// zero time removes it. Both are net.Conn's stated behaviour and neither may
// take the association with it.
func TestReadDeadlineBoundaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3223)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	t.Run("a deadline in the past reports at once", func(t *testing.T) {
		if err := cliConn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		start := time.Now()
		if _, err := cliConn.Read(make([]byte, 8)); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("Read = %v, want os.ErrDeadlineExceeded", err)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("an already-expired deadline took %v to report", d)
		}
	})

	t.Run("every read entry point honours it", func(t *testing.T) {
		if err := cliConn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		if _, err := cliConn.ReadPD(); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("ReadPD = %v, want os.ErrDeadlineExceeded", err)
		}
		if _, err := cliConn.ReadData(); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("ReadData = %v, want os.ErrDeadlineExceeded", err)
		}
	})

	t.Run("SetDeadline arms the read side too", func(t *testing.T) {
		if err := cliConn.SetDeadline(time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("SetDeadline: %v", err)
		}
		if _, err := cliConn.ReadData(); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("ReadData after SetDeadline = %v, want os.ErrDeadlineExceeded", err)
		}
	})

	t.Run("the zero time removes it", func(t *testing.T) {
		if err := cliConn.SetReadDeadline(time.Time{}); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		pd := params.NewProtocolData(
			0x22222222, 0x11111111, params.ServiceIndSCCP, 0, 0, 1, []byte("ok"))
		if _, err := srvConn.WritePDWithRoutingContext(pd, 1); err != nil {
			t.Fatalf("peer write: %v", err)
		}
		got, err := cliConn.ReadPD()
		if err != nil {
			t.Fatalf("ReadPD with no deadline: %v", err)
		}
		if string(got.Data) != "ok" {
			t.Errorf("payload = %q, want %q", got.Data, "ok")
		}
	})

	// After all of that the association is still the one we started with.
	if state := cliConn.State(); state != StateASPActive {
		t.Errorf("state = %v, want %v", state, StateASPActive)
	}
}

// The same on an accepted association. SetReadDeadline has no mode branch, so
// this exercises identical code — but "identical code, therefore fine" is an
// argument, not a run, and an SGP reading from many ASPs is the side that most
// plausibly sets one.
func TestReadDeadlineOnAnAcceptedAssociation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3229)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	if err := srvConn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := srvConn.Read(make([]byte, 16)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read = %v, want os.ErrDeadlineExceeded", err)
	}
	if state := srvConn.State(); state != StateASPActive {
		t.Errorf("accepted association went to %v on a read deadline, want %v",
			state, StateASPActive)
	}
	select {
	case <-srvConn.Done():
		t.Fatalf("the accepted association was closed by a read deadline: %v", srvConn.Err())
	default:
	}

	// Still carries traffic from the ASP afterwards.
	if err := srvConn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clearing the deadline: %v", err)
	}
	if _, err := cliConn.WritePDWithRoutingContext(params.NewProtocolData(
		0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("up")), 1); err != nil {
		t.Fatalf("association write: %v", err)
	}
	got, err := srvConn.ReadPD()
	if err != nil {
		t.Fatalf("ReadPD after the deadline was cleared: %v", err)
	}
	if string(got.Data) != "up" {
		t.Errorf("payload = %q, want %q", got.Data, "up")
	}
}
