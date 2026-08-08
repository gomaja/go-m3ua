// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// One SGP listener serving several ASPs at once is the ordinary production
// deployment: a single M3UA port fronting many remote ASPs, each with its own
// point code and routing context. Nothing in the suite exercised it, and the
// association state was reachable through the *Config every Conn shares, so a
// second Accept silently rebound the first Conn's socket.
//
// These tests are socket-backed and therefore only run where SCTP exists
// (Linux); they skip elsewhere, as the rest of the socket tests do.

// mcAddr builds an SCTPAddr from one or more dotted-quad addresses.
func mcAddr(port int, ips ...string) *sctp.SCTPAddr {
	a := &sctp.SCTPAddr{Port: port}
	for _, s := range ips {
		a.IPAddrs = append(a.IPAddrs, net.IPAddr{IP: net.ParseIP(s)})
	}
	return a
}

// mcServerConfig is the SGP-side Config the listener is built with. It is
// deliberately shared across every accepted association, which is how a server
// is written in practice and what the defect fed on.
func mcServerConfig() *Config {
	cfg := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.AspIdentifier = nil
	cfg.CorrelationID = nil
	return cfg
}

// mcClientConfig is an ASP-side Config with its own originating point code, so
// the two ASPs in these tests are distinguishable.
func mcClientConfig(opc uint32) *Config {
	cfg := NewClientConfig(
		&HeartbeatInfo{Enabled: false},
		opc, 0x22222222, opc, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.CorrelationID = nil
	return cfg
}

// mcListen starts an M3UA listener, skipping the test where SCTP is absent.
func mcListen(t *testing.T, laddr *sctp.SCTPAddr) *Listener {
	t.Helper()

	ln, err := Listen("m3ua", laddr, NewListenerConfig(mcServerConfig()))
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// mcASP dials one ASP into the listener and returns both ends of it.
type mcASP struct {
	client *Conn // the ASP's own Conn
	server *Conn // the Conn the listener accepted for it
}

// mcConnect brings up n ASPs against ln, one at a time so each Accept is
// unambiguously paired with the Dial that caused it, and returns them in
// connection order.
func mcConnect(t *testing.T, ctx context.Context, ln *Listener, raddr *sctp.SCTPAddr, clientIPs []string, port int) []mcASP {
	t.Helper()

	type accepted struct {
		conn *Conn
		err  error
	}
	accepts := make(chan accepted, len(clientIPs))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range clientIPs {
			c, err := ln.Accept(ctx)
			accepts <- accepted{c, err}
		}
	}()
	// Close the listener before waiting for that goroutine, and bound the wait.
	//
	// A bare t.Cleanup(wg.Wait) here deadlocks the whole test binary. Cleanups
	// run LIFO, so one registered here runs BEFORE the ln.Close that mcListen
	// registered earlier. Any exit that leaves fewer peers connected than this
	// loop expects — a t.Fatalf on a failed Dial, a cancelled ctx — parks the
	// goroutine in ln.Accept, and Accept's contract is explicit that ctx will
	// not rescue it: "Cancelling ctx does not interrupt an Accept that is
	// blocked waiting for a peer to connect — only Close does" (server.go).
	// Waiting first therefore waits forever on a goroutine whose only unblock
	// is queued behind the wait. The observed symptom is not a failed
	// assertion but a ten-minute silence ending in "panic: test timed out",
	// with every later test in the package unreported — which is how this hid:
	// it looks like whichever test the timeout happens to name.
	//
	// Closing here breaks the cycle. mcListen's cleanup then runs as a second
	// Close, which is harmless: l.conns is already nil and the error is
	// discarded.
	t.Cleanup(func() {
		_ = ln.Close()

		// Bounded, so that if Accept ever stops honouring Close this reports a
		// failure naming the real problem instead of hanging the suite again.
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("the accept goroutine was still parked 10s after the listener closed; " +
				"Listener.Close no longer interrupts a blocked Accept")
		}
	})

	asps := make([]mcASP, 0, len(clientIPs))
	for i, ip := range clientIPs {
		cli, err := Dial(ctx, "m3ua", mcAddr(port+1+i, ip), raddr, mcClientConfig(0xAA000000+uint32(i)))
		if err != nil {
			t.Fatalf("ASP #%d failed to establish: %v", i+1, err)
		}
		t.Cleanup(func() { _ = cli.Close() })

		select {
		case a := <-accepts:
			if a.err != nil {
				t.Fatalf("Accept for ASP #%d: %v", i+1, a.err)
			}
			t.Cleanup(func() { _ = a.conn.Close() })
			// Both ends coordinate two Routing Contexts, so each has to name
			// the one its DATA belongs to (Section 3.3.1). These tests are
			// about the association, not about distribution across Application
			// Servers, so both pick the same one.
			for _, c := range []*Conn{cli, a.conn} {
				if err := c.SelectRoutingContext(1); err != nil {
					t.Fatalf("SelectRoutingContext: %v", err)
				}
			}
			asps = append(asps, mcASP{client: cli, server: a.conn})
		case <-time.After(15 * time.Second):
			t.Fatalf("Accept for ASP #%d never returned", i+1)
		}
	}
	return asps
}

// readWithin reads one payload from c, or reports why it could not.
func readWithin(t *testing.T, c *Conn, d time.Duration) (string, error) {
	t.Helper()

	type result struct {
		s   string
		err error
	}
	out := make(chan result, 1)
	go func() {
		buf := make([]byte, 4096)
		n, err := c.Read(buf)
		out <- result{string(buf[:n]), err}
	}()

	select {
	case r := <-out:
		return r.s, r.err
	case <-time.After(d):
		return "", fmt.Errorf("nothing arrived within %v", d)
	}
}

// A second Accept must not disturb the association the first Accept returned.
//
// Accept assigned the accepted socket to the *Config shared by every Conn the
// listener produces, so Accept #2 rebound Conn #1 to ASP #2's association:
// Conn #1 then reported ASP #2's address, read ASP #2's messages, wrote to ASP
// #2, and closing either Conn closed the other's socket.
func TestSecondAcceptDoesNotRebindTheFirstConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3010
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	// Each accepted Conn must still be bound to the association it was
	// accepted on: its remote address is its own ASP's local address.
	for i, a := range asps {
		want := a.client.LocalAddr().String()
		got := a.server.RemoteAddr().String()
		if got != want {
			t.Errorf("server Conn #%d RemoteAddr = %s, want %s (the ASP it was accepted for)", i+1, got, want)
		}
	}
	if a, b := asps[0].server.RemoteAddr().String(), asps[1].server.RemoteAddr().String(); a == b {
		t.Fatalf("both accepted Conns report the same peer %s: the second Accept rebound the first Conn", a)
	}
}

// Payloads must reach the Conn belonging to the ASP that sent them. This is the
// consequence that matters operationally: with the socket shared, one ASP's
// traffic surfaced on another ASP's Conn, so an SGP would attribute signalling
// to the wrong peer.
func TestEachASPsTrafficArrivesOnItsOwnConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3013
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	payloads := []string{"from-asp-1", "from-asp-2"}
	for i, a := range asps {
		if _, err := a.client.Write([]byte(payloads[i])); err != nil {
			t.Fatalf("ASP #%d write: %v", i+1, err)
		}
	}

	for i, a := range asps {
		got, err := readWithin(t, a.server, 5*time.Second)
		if err != nil {
			t.Errorf("server Conn #%d read: %v", i+1, err)
			continue
		}
		if got != payloads[i] {
			t.Errorf("server Conn #%d read %q, want %q: traffic was delivered to the wrong ASP's Conn", i+1, got, payloads[i])
		}
	}
}

// Writes must reach the ASP the Conn belongs to, in the reverse direction.
func TestServerWritesReachTheOwningASP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3016
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	payloads := []string{"to-asp-1", "to-asp-2"}
	for i, a := range asps {
		if _, err := a.server.Write([]byte(payloads[i])); err != nil {
			t.Fatalf("server Conn #%d write: %v", i+1, err)
		}
	}

	for i, a := range asps {
		got, err := readWithin(t, a.client, 5*time.Second)
		if err != nil {
			t.Errorf("ASP #%d read: %v", i+1, err)
			continue
		}
		if got != payloads[i] {
			t.Errorf("ASP #%d read %q, want %q: the SGP wrote to the wrong association", i+1, got, payloads[i])
		}
	}
}

// Closing one accepted Conn must take down only that ASP's association. With a
// shared socket, closing either Conn closed the single fd both were using, so
// one ASP disconnecting killed every other ASP on the listener.
func TestClosingOneConnLeavesTheOtherASPUsable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3019
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	if err := asps[0].server.Close(); err != nil {
		t.Fatalf("closing server Conn #1: %v", err)
	}

	if got := asps[1].server.State(); got != StateAspActive {
		t.Errorf("server Conn #2 state = %v after closing Conn #1, want %v", got, StateAspActive)
	}
	if _, err := asps[1].client.Write([]byte("still-here")); err != nil {
		t.Fatalf("ASP #2 write after closing server Conn #1: %v", err)
	}
	got, err := readWithin(t, asps[1].server, 5*time.Second)
	if err != nil {
		t.Fatalf("server Conn #2 read after closing Conn #1: %v", err)
	}
	if got != "still-here" {
		t.Errorf("server Conn #2 read %q, want %q", got, "still-here")
	}
}

// The two associations must be independent under concurrent traffic, with no
// shared mutable state between them. Run under -race this is the test that
// catches a field written by one Conn's goroutines and read by another's.
func TestConcurrentTrafficOnTwoASPsIsRaceFree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3022
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	const rounds = 20
	var wg sync.WaitGroup
	for i, a := range asps {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := fmt.Fprintf(a.client, "asp%d-%d", i+1, r); err != nil {
					t.Errorf("ASP #%d write %d: %v", i+1, r, err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := fmt.Fprintf(a.server, "sgp%d-%d", i+1, r); err != nil {
					t.Errorf("server Conn #%d write %d: %v", i+1, r, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Drain what arrived and check every payload landed on the right side: a
	// message tagged for one ASP must never surface on the other's Conn.
	for i, a := range asps {
		wantPrefix := fmt.Sprintf("asp%d-", i+1)
		for r := 0; r < rounds; r++ {
			got, err := readWithin(t, a.server, 5*time.Second)
			if err != nil {
				t.Errorf("server Conn #%d: only %d of %d payloads arrived (%v)", i+1, r, rounds, err)
				break
			}
			if !strings.HasPrefix(got, wantPrefix) {
				t.Fatalf("server Conn #%d received %q, want a payload prefixed %q", i+1, got, wantPrefix)
			}
		}
	}
}

// Accept must be safe to call from several goroutines at once. A single-threaded
// accept loop head-of-line blocks every waiting ASP behind the establishment of
// the one in front of it, so a server that wants to bring peers up in parallel
// needs concurrent Accepts not to corrupt each other.
func TestConcurrentAcceptsAreIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3025
	const peers = 3
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	raddr := mcAddr(port, "127.0.0.1")

	type accepted struct {
		conn *Conn
		err  error
	}
	accepts := make(chan accepted, peers)
	for i := 0; i < peers; i++ {
		go func() {
			c, err := ln.Accept(ctx)
			accepts <- accepted{c, err}
		}()
	}

	clientIPs := []string{"127.0.0.2", "127.0.0.3", "127.0.0.4"}
	clients := make([]*Conn, 0, peers)
	var dialWG sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < peers; i++ {
		dialWG.Add(1)
		go func() {
			defer dialWG.Done()
			cli, err := Dial(ctx, "m3ua", mcAddr(port+1+i, clientIPs[i]), raddr, mcClientConfig(0xBB000000+uint32(i)))
			if err != nil {
				t.Errorf("ASP #%d dial: %v", i+1, err)
				return
			}
			mu.Lock()
			clients = append(clients, cli)
			mu.Unlock()
		}()
	}
	dialWG.Wait()
	for _, c := range clients {
		defer func() { _ = c.Close() }()
	}

	seen := map[string]bool{}
	for i := 0; i < peers; i++ {
		select {
		case a := <-accepts:
			if a.err != nil {
				t.Fatalf("concurrent Accept #%d: %v", i+1, a.err)
			}
			defer func() { _ = a.conn.Close() }()
			remote := a.conn.RemoteAddr().String()
			if seen[remote] {
				t.Errorf("two accepted Conns report the same peer %s", remote)
			}
			seen[remote] = true
		case <-time.After(20 * time.Second):
			t.Fatalf("only %d of %d concurrent Accepts returned", i, peers)
		}
	}
}
