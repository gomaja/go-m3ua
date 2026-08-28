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
// association state was reachable through AssociationConfig shared by every
// Association, so a second Accept silently rebound the first SCTP association.
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

// mcSGPConfig is the SGP AssociationConfig used by the Listener. It is
// deliberately shared across every accepted association.
func mcSGPConfig() *AssociationConfig {
	cfg := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.ASPIdentifier = nil
	cfg.CorrelationID = nil
	return cfg
}

// mcASPConfig is an ASP AssociationConfig with its own originating point code, so
// the two ASPs in these tests are distinguishable.
func mcASPConfig(opc uint32) *AssociationConfig {
	cfg := newASPAssociationConfigForTest(
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

	ln, err := listenSGP("m3ua", laddr, NewListenerConfig(mcSGPConfig()))
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
	asp *Association // the ASP role
	sgp *Association // the SGP role
}

// mcConnect brings up n ASPs against ln, one at a time so each Accept is
// unambiguously paired with the Dial that caused it, and returns them in
// connection order.
func mcConnect(t *testing.T, ctx context.Context, ln *Listener, raddr *sctp.SCTPAddr, aspIPs []string, port int) []mcASP {
	t.Helper()

	type accepted struct {
		association *Association
		err         error
	}
	accepts := make(chan accepted, len(aspIPs))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range aspIPs {
			association, err := ln.Accept(ctx)
			accepts <- accepted{association, err}
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
	// blocked waiting for a peer to connect — only Close does" (listener.go).
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

	asps := make([]mcASP, 0, len(aspIPs))
	for i, ip := range aspIPs {
		asp, err := dialASP(ctx, "m3ua", mcAddr(port+1+i, ip), raddr, mcASPConfig(0xAA000000+uint32(i)))
		if err != nil {
			t.Fatalf("ASP #%d failed to establish: %v", i+1, err)
		}
		t.Cleanup(func() { _ = asp.Close() })

		select {
		case a := <-accepts:
			if a.err != nil {
				t.Fatalf("Accept for ASP #%d: %v", i+1, a.err)
			}
			t.Cleanup(func() { _ = a.association.Close() })
			// Both ends coordinate two Routing Contexts, so each has to name
			// the one its DATA belongs to (Section 3.3.1). These tests are
			// about the association, not about distribution across Application
			// Servers, so both pick the same one.
			for _, association := range []*Association{asp, a.association} {
				if err := association.SelectRoutingContext(1); err != nil {
					t.Fatalf("SelectRoutingContext: %v", err)
				}
			}
			asps = append(asps, mcASP{asp: asp, sgp: a.association})
		case <-time.After(15 * time.Second):
			t.Fatalf("Accept for ASP #%d never returned", i+1)
		}
	}
	return asps
}

// readWithin reads one payload from association, or reports why it could not.
func readWithin(t *testing.T, association *Association, d time.Duration) (string, error) {
	t.Helper()

	type result struct {
		s   string
		err error
	}
	out := make(chan result, 1)
	go func() {
		buf := make([]byte, 4096)
		n, err := association.Read(buf)
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
// Accept assigned the accepted socket to AssociationConfig shared by every
// Association. Accept #2 therefore rebound association #1 to ASP #2.
func TestSecondAcceptDoesNotRebindTheFirstAssociation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3010
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	// Each accepted Association must remain bound to the SCTP association it was
	// accepted on: its remote address is its own ASP's local address.
	for i, a := range asps {
		want := a.asp.LocalAddr().String()
		got := a.sgp.RemoteAddr().String()
		if got != want {
			t.Errorf("SGP association #%d RemoteAddr = %s, want %s", i+1, got, want)
		}
	}
	if a, b := asps[0].sgp.RemoteAddr().String(), asps[1].sgp.RemoteAddr().String(); a == b {
		t.Fatalf("both accepted Associations report peer %s: the second Accept rebound the first", a)
	}
}

// Payloads must reach the Association belonging to the ASP that sent them.
func TestEachASPsTrafficArrivesOnItsOwnAssociation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3013
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	payloads := []string{"from-asp-1", "from-asp-2"}
	for i, a := range asps {
		if _, err := a.asp.Write([]byte(payloads[i])); err != nil {
			t.Fatalf("ASP #%d write: %v", i+1, err)
		}
	}

	for i, a := range asps {
		got, err := readWithin(t, a.sgp, 5*time.Second)
		if err != nil {
			t.Errorf("SGP association #%d read: %v", i+1, err)
			continue
		}
		if got != payloads[i] {
			t.Errorf("SGP association #%d read %q, want %q", i+1, got, payloads[i])
		}
	}
}

// SGP writes must reach the ASP served by that Association.
func TestSGPWritesReachTheOwningASP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3016
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	payloads := []string{"to-asp-1", "to-asp-2"}
	for i, a := range asps {
		if _, err := a.sgp.Write([]byte(payloads[i])); err != nil {
			t.Fatalf("SGP association #%d write: %v", i+1, err)
		}
	}

	for i, a := range asps {
		got, err := readWithin(t, a.asp, 5*time.Second)
		if err != nil {
			t.Errorf("ASP #%d read: %v", i+1, err)
			continue
		}
		if got != payloads[i] {
			t.Errorf("ASP #%d read %q, want %q: the SGP wrote to the wrong association", i+1, got, payloads[i])
		}
	}
}

// Closing one accepted Association must take down only that ASP.
func TestClosingOneAssociationLeavesTheOtherASPUsable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3019
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	if err := asps[0].sgp.Close(); err != nil {
		t.Fatalf("closing SGP association #1: %v", err)
	}

	if got := asps[1].sgp.State(); got != StateASPActive {
		t.Errorf("SGP association #2 state = %v after closing #1, want %v", got, StateASPActive)
	}
	if _, err := asps[1].asp.Write([]byte("still-here")); err != nil {
		t.Fatalf("ASP #2 write after closing SGP association #1: %v", err)
	}
	got, err := readWithin(t, asps[1].sgp, 5*time.Second)
	if err != nil {
		t.Fatalf("SGP association #2 read after closing #1: %v", err)
	}
	if got != "still-here" {
		t.Errorf("SGP association #2 read %q, want %q", got, "still-here")
	}
}

// The two associations must be independent under concurrent traffic, with no
// shared mutable state between them. Run under -race this is the test that
// catches a field written by one Association's goroutines and read by another.
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
				if _, err := fmt.Fprintf(a.asp, "asp%d-%d", i+1, r); err != nil {
					t.Errorf("ASP #%d write %d: %v", i+1, r, err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := fmt.Fprintf(a.sgp, "sgp%d-%d", i+1, r); err != nil {
					t.Errorf("SGP association #%d write %d: %v", i+1, r, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Drain what arrived and check every payload landed on the right side: a
	// message tagged for one ASP must never surface on the other's Association.
	for i, a := range asps {
		wantPrefix := fmt.Sprintf("asp%d-", i+1)
		for r := 0; r < rounds; r++ {
			got, err := readWithin(t, a.sgp, 5*time.Second)
			if err != nil {
				t.Errorf("SGP association #%d: only %d of %d payloads arrived (%v)", i+1, r, rounds, err)
				break
			}
			if !strings.HasPrefix(got, wantPrefix) {
				t.Fatalf("SGP association #%d received %q, want a payload prefixed %q", i+1, got, wantPrefix)
			}
		}
	}
}

// Accept must be safe to call from several goroutines at once. A single-threaded
// accept loop head-of-line blocks every waiting ASP behind the establishment of
// the one in front of it, so an endpoint that brings peers up in parallel
// needs concurrent Accepts not to corrupt each other.
func TestConcurrentAcceptsAreIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3025
	const peers = 3
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	raddr := mcAddr(port, "127.0.0.1")

	type accepted struct {
		association *Association
		err         error
	}
	accepts := make(chan accepted, peers)
	for i := 0; i < peers; i++ {
		go func() {
			c, err := ln.Accept(ctx)
			accepts <- accepted{c, err}
		}()
	}

	aspIPs := []string{"127.0.0.2", "127.0.0.3", "127.0.0.4"}
	aspAssociations := make([]*Association, 0, peers)
	var dialWG sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < peers; i++ {
		dialWG.Add(1)
		go func() {
			defer dialWG.Done()
			asp, err := dialASP(ctx, "m3ua", mcAddr(port+1+i, aspIPs[i]), raddr, mcASPConfig(0xBB000000+uint32(i)))
			if err != nil {
				t.Errorf("ASP #%d dial: %v", i+1, err)
				return
			}
			mu.Lock()
			aspAssociations = append(aspAssociations, asp)
			mu.Unlock()
		}()
	}
	dialWG.Wait()
	for _, association := range aspAssociations {
		defer func() { _ = association.Close() }()
	}

	seen := map[string]bool{}
	for i := 0; i < peers; i++ {
		select {
		case a := <-accepts:
			if a.err != nil {
				t.Fatalf("concurrent Accept #%d: %v", i+1, a.err)
			}
			defer func() { _ = a.association.Close() }()
			remote := a.association.RemoteAddr().String()
			if seen[remote] {
				t.Errorf("two accepted Associations report the same peer %s", remote)
			}
			seen[remote] = true
		case <-time.After(20 * time.Second):
			t.Fatalf("only %d of %d concurrent Accepts returned", i, peers)
		}
	}
}
