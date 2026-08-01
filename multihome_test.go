// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

// Multi-homing is SCTP's defining feature over TCP and the reason M3UA runs on
// it: an association binds several addresses at each end and survives losing a
// path. go-m3ua takes *sctp.SCTPAddr straight from the caller, so support is a
// matter of passing it through unchanged — which nothing verified.
//
// These tests need several usable loopback addresses and skip without them.
// The container adds them (local/run-tests.sh); on a bare host:
//
//	ip addr add 127.0.0.2/8 dev lo
//
// A path *failure* cannot be produced on loopback — traffic between two of a
// host's own addresses never leaves lo, so dropping one address is not a path
// failure and the kernel never marks the path down. Real failover therefore
// runs outside the Go suite, across two containers on two networks
// (local/validate-failover.sh).

// usableLoopbacks returns the 127.0.0.x addresses that can actually be bound,
// determined by binding them rather than by parsing interface configuration.
func usableLoopbacks(t *testing.T) []string {
	t.Helper()

	var ok []string
	for _, s := range []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"} {
		addr := &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.ParseIP(s)}}, Port: 0}
		ln, err := sctp.ListenSCTP("sctp4", addr)
		if err != nil {
			if isSCTPUnsupported(err) {
				t.Skipf("skipping socket-backed test: %v", err)
			}
			continue
		}
		_ = ln.Close()
		ok = append(ok, s)
	}
	return ok
}

// requireLoopbacks skips unless at least n loopback addresses are usable.
func requireLoopbacks(t *testing.T, n int) []string {
	t.Helper()

	got := usableLoopbacks(t)
	if len(got) < n {
		t.Skipf("only %d usable loopback addresses (%v); this test needs %d. Add them with: ip addr add 127.0.0.N/8 dev lo",
			len(got), got, n)
	}
	return got
}

// addrsOf splits an SCTPAddr's String() form ("a/b/c:port") into its addresses.
func addrsOf(a net.Addr) []string {
	s := a.String()
	if i := strings.LastIndex(s, ":"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, "/")
	sort.Strings(parts)
	return parts
}

func sameAddrs(got []string, want []string) bool {
	w := append([]string(nil), want...)
	sort.Strings(w)
	if len(got) != len(w) {
		return false
	}
	for i := range got {
		if got[i] != w[i] {
			return false
		}
	}
	return true
}

// The base case: an association whose two ends each bind several addresses,
// with both ends learning the other's full set. The address exchange happens in
// the INIT/INIT-ACK handshake, so an end that flattened its SCTPAddr shows up
// here as a peer that knows fewer addresses than were bound — while a
// single-path data test still passes.
func TestMultihomedAssociationKeepsEveryAddress(t *testing.T) {
	avail := requireLoopbacks(t, 4)
	srvIPs, cliIPs := avail[0:2], avail[2:4]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const port = 3030
	ln := mcListen(t, mcAddr(port, srvIPs...))

	if got := addrsOf(ln.Addr()); !sameAddrs(got, srvIPs) {
		t.Errorf("listener Addr() = %v, want %v: Listen did not bind every address", got, srvIPs)
	}

	type accepted struct {
		conn *Conn
		err  error
	}
	accepts := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept(ctx)
		accepts <- accepted{c, err}
	}()

	cli, err := Dial(ctx, "m3ua", mcAddr(port+1, cliIPs...), mcAddr(port, srvIPs...), mcClientConfig(0xAA000001))
	if err != nil {
		t.Fatalf("multi-homed Dial: %v", err)
	}
	defer func() { _ = cli.Close() }()

	var srv *Conn
	select {
	case a := <-accepts:
		if a.err != nil {
			t.Fatalf("multi-homed Accept: %v", a.err)
		}
		srv = a.conn
	case <-time.After(15 * time.Second):
		t.Fatal("Accept never returned for the multi-homed peer")
	}
	defer func() { _ = srv.Close() }()

	for _, tc := range []struct {
		name string
		got  []string
		want []string
	}{
		{"client LocalAddr", addrsOf(cli.LocalAddr()), cliIPs},
		{"client RemoteAddr", addrsOf(cli.RemoteAddr()), srvIPs},
		{"server LocalAddr", addrsOf(srv.LocalAddr()), srvIPs},
		{"server RemoteAddr", addrsOf(srv.RemoteAddr()), cliIPs},
	} {
		if !sameAddrs(tc.got, tc.want) {
			t.Errorf("%s = %v, want %v: the association did not negotiate every bound address", tc.name, tc.got, tc.want)
		}
	}
}

// M3UA payload must round-trip over a multi-homed association in both
// directions. The address negotiation above proves the handshake; this proves
// the data path the SGP actually uses.
func TestMultihomedAssociationCarriesPayloadBothWays(t *testing.T) {
	avail := requireLoopbacks(t, 4)
	srvIPs, cliIPs := avail[0:2], avail[2:4]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const port = 3032
	ln := mcListen(t, mcAddr(port, srvIPs...))

	type accepted struct {
		conn *Conn
		err  error
	}
	accepts := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept(ctx)
		accepts <- accepted{c, err}
	}()

	cli, err := Dial(ctx, "m3ua", mcAddr(port+1, cliIPs...), mcAddr(port, srvIPs...), mcClientConfig(0xAA000002))
	if err != nil {
		t.Fatalf("multi-homed Dial: %v", err)
	}
	defer func() { _ = cli.Close() }()

	a := <-accepts
	if a.err != nil {
		t.Fatalf("multi-homed Accept: %v", a.err)
	}
	srv := a.conn
	defer func() { _ = srv.Close() }()

	// Both ends coordinate two Routing Contexts, so each DATA has to name the
	// one identifying its traffic flow (RFC 4666 Section 3.3.1). This test is
	// about multi-homing, so both ends pick the same one.
	for _, c := range []*Conn{cli, srv} {
		if err := c.SelectRoutingContext(1); err != nil {
			t.Fatalf("SelectRoutingContext: %v", err)
		}
	}

	for _, tc := range []struct {
		name       string
		from, to   *Conn
		payload    string
		wantOnRead string
	}{
		{"ASP to SGP", cli, srv, "up-the-multihomed-path", "up-the-multihomed-path"},
		{"SGP to ASP", srv, cli, "down-the-multihomed-path", "down-the-multihomed-path"},
	} {
		if _, err := tc.from.Write([]byte(tc.payload)); err != nil {
			t.Fatalf("%s write: %v", tc.name, err)
		}
		got, err := readWithin(t, tc.to, 5*time.Second)
		if err != nil {
			t.Fatalf("%s read: %v", tc.name, err)
		}
		if got != tc.wantOnRead {
			t.Errorf("%s read %q, want %q", tc.name, got, tc.wantOnRead)
		}
	}
}

// A multi-homed listener must serve several multi-homed ASPs at once, which is
// how the two features actually meet in production: one SGP bound to several
// addresses, each ASP likewise. Each association must still keep its own
// address set rather than inheriting the last one accepted.
func TestMultihomedListenerServesSeveralASPs(t *testing.T) {
	avail := requireLoopbacks(t, 4)
	srvIPs := avail[0:2]
	// Two ASPs, each single-homed onto a distinct address, so their sets are
	// distinguishable; the SGP side stays multi-homed.
	aspIPs := [][]string{{avail[2]}, {avail[3]}}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3034
	ln := mcListen(t, mcAddr(port, srvIPs...))

	type accepted struct {
		conn *Conn
		err  error
	}
	accepts := make(chan accepted, len(aspIPs))
	go func() {
		for range aspIPs {
			c, err := ln.Accept(ctx)
			accepts <- accepted{c, err}
		}
	}()

	for i, ips := range aspIPs {
		cli, err := Dial(ctx, "m3ua", mcAddr(port+1+i, ips...), mcAddr(port, srvIPs...), mcClientConfig(0xCC000000+uint32(i)))
		if err != nil {
			t.Fatalf("ASP #%d multi-homed dial: %v", i+1, err)
		}
		defer func() { _ = cli.Close() }()

		select {
		case a := <-accepts:
			if a.err != nil {
				t.Fatalf("Accept for ASP #%d: %v", i+1, a.err)
			}
			defer func() { _ = a.conn.Close() }()

			if got := addrsOf(a.conn.LocalAddr()); !sameAddrs(got, srvIPs) {
				t.Errorf("server Conn #%d LocalAddr = %v, want the listener's %v", i+1, got, srvIPs)
			}
			if got := addrsOf(a.conn.RemoteAddr()); !sameAddrs(got, ips) {
				t.Errorf("server Conn #%d RemoteAddr = %v, want ASP #%d's %v", i+1, got, i+1, ips)
			}

			// One of the two coordinated Routing Contexts names the flow
			// (RFC 4666 Section 3.3.1).
			if err := cli.SelectRoutingContext(1); err != nil {
				t.Fatalf("SelectRoutingContext: %v", err)
			}

			payload := fmt.Sprintf("mh-asp-%d", i+1)
			if _, err := cli.Write([]byte(payload)); err != nil {
				t.Fatalf("ASP #%d write: %v", i+1, err)
			}
			got, err := readWithin(t, a.conn, 5*time.Second)
			if err != nil {
				t.Fatalf("server Conn #%d read: %v", i+1, err)
			}
			if got != payload {
				t.Errorf("server Conn #%d read %q, want %q", i+1, got, payload)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("Accept for ASP #%d never returned", i+1)
		}
	}
}
