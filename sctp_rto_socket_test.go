// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

// These tests open SCTP sockets. Distinct values detect a wrong source even
// when the network namespace uses custom sysctls.
func TestSCTPRTOSocketConfiguration(t *testing.T) {
	ips := requireLoopbacks(t, 2)
	for _, name := range []string{"rto_initial", "rto_min", "rto_max", "association_max_retrans"} {
		b, err := os.ReadFile("/proc/sys/net/sctp/" + name)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("sysctl %s=%s", name, b)
	}
	for _, tt := range []struct {
		name     string
		selected *sctp.RTOInfo
		want     sctp.RTOInfo
	}{
		{"default", nil, sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 400 * time.Millisecond, Max: 3500 * time.Millisecond}},
		{"selected", &sctp.RTOInfo{Initial: 1500 * time.Millisecond, Min: 300 * time.Millisecond, Max: 2500 * time.Millisecond}, sctp.RTOInfo{Initial: 1500 * time.Millisecond, Min: 300 * time.Millisecond, Max: 2500 * time.Millisecond}},
		{"selected zeros", &sctp.RTOInfo{Min: 300 * time.Millisecond}, sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 300 * time.Millisecond, Max: 3500 * time.Millisecond}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			sgp, asp := mcSGPConfig(), mcASPConfig(mcASPPointCode)
			sgp.RTOInfo = &sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 400 * time.Millisecond, Max: 3500 * time.Millisecond}
			sgp.AssociationMaxRetrans = 7
			asp.RTOInfo = &sctp.RTOInfo{Initial: 1700 * time.Millisecond, Min: 500 * time.Millisecond, Max: 2700 * time.Millisecond}
			asp.AssociationMaxRetrans = 6
			type observed struct {
				status *AssociationStatus
				err    error
			}
			seen := make(chan observed, 1)
			sgp.Subscribe = func(a *Association) { st, err := a.AssociationStatus(); seen <- observed{st, err} }
			lc := NewListenerConfig(sgp)
			if tt.selected != nil {
				selected := snapshotAssociationConfig(sgp)
				selected.RTOInfo = tt.selected
				selected.AssociationMaxRetrans = 8
				lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) { return selected, nil }
			}
			ln, err := listenSGP("m3ua", mcAddr(0, ips[0]), lc)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			type result struct {
				a   *Association
				err error
			}
			accepted := make(chan result, 1)
			go func() { a, e := ln.Accept(ctx); accepted <- result{a, e} }()
			cli, err := dialASP(ctx, "m3ua", mcAddr(0, ips[1]), ln.Addr().(*sctp.Addr), asp)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cli.Close() }()
			srv := <-accepted
			if srv.err != nil {
				t.Fatal(srv.err)
			}
			defer func() { _ = srv.a.Close() }()
			before := <-seen
			if before.err != nil {
				t.Fatal(before.err)
			}
			if before.status.RTOInfo != tt.want {
				t.Fatalf("pre-handshake accepted RTO=%+v want=%+v", before.status.RTOInfo, tt.want)
			}
			t.Logf("right after accept: primary RTO=%v association RTO=%+v", before.status.Primary.RTO, before.status.RTOInfo)
			if tt.selected == nil && before.status.Primary.RTO != 2200*time.Millisecond {
				t.Fatalf("listener did not create path with its Initial: %v", before.status.Primary.RTO)
			}
			for i, a := range []*Association{cli, srv.a} {
				want, max := *asp.RTOInfo, uint16(6)
				if i == 1 {
					want, max = tt.want, 7
					if tt.selected != nil {
						max = 8
					}
				}
				checkSCTPRTOStatus(t, a, want, max)
				changed := sctp.RTOInfo{Initial: 1800 * time.Millisecond, Min: 600 * time.Millisecond, Max: 2800 * time.Millisecond}
				if e := a.SetSCTPRTOInfo(&changed); e != nil {
					t.Fatal(e)
				}
				if e := a.SetSCTPAssociationMaxRetrans(3); e != nil {
					t.Fatal(e)
				}
				checkSCTPRTOStatus(t, a, changed, 3)
				if e := a.SetSCTPRTOInfo(&sctp.RTOInfo{}); e != nil {
					t.Fatal(e)
				}
				if e := a.SetSCTPAssociationMaxRetrans(0); e != nil {
					t.Fatal(e)
				}
				checkSCTPRTOStatus(t, a, changed, 3)
				for _, r := range []*sctp.RTOInfo{nil, {Min: 4 * time.Second}, {Min: 4 * time.Second, Max: 3 * time.Second}} {
					if e := a.SetSCTPRTOInfo(r); !errors.Is(e, syscall.EINVAL) {
						t.Fatalf("invalid RTO error=%v", e)
					}
				}
			}
			if err := cli.Close(); err != nil {
				t.Fatal(err)
			}
			if _, e := cli.SCTPRTOInfo(); !errors.Is(e, net.ErrClosed) {
				t.Fatalf("closed RTO: %v", e)
			}
			if e := cli.SetSCTPRTOInfo(asp.RTOInfo); !errors.Is(e, net.ErrClosed) {
				t.Fatalf("closed set RTO: %v", e)
			}
			if _, e := cli.SCTPAssociationMaxRetrans(); !errors.Is(e, net.ErrClosed) {
				t.Fatalf("closed max: %v", e)
			}
			if e := cli.SetSCTPAssociationMaxRetrans(3); !errors.Is(e, net.ErrClosed) {
				t.Fatalf("closed set max: %v", e)
			}
		})
	}
}

func checkSCTPRTOStatus(t *testing.T, a *Association, want sctp.RTOInfo, max uint16) {
	t.Helper()
	r, err := a.SCTPRTOInfo()
	if err != nil {
		t.Fatal(err)
	}
	if *r != want {
		t.Fatalf("RTOInfo=%+v want=%+v", r, want)
	}
	n, err := a.SCTPAssociationMaxRetrans()
	if err != nil {
		t.Fatal(err)
	}
	if n != max {
		t.Fatalf("MaxRetrans=%d want=%d", n, max)
	}
	st, err := a.AssociationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if st.RTOInfo != want || st.AssociationMaxRetrans != max {
		t.Fatalf("status RTO=%+v max=%d want=%+v %d", st.RTOInfo, st.AssociationMaxRetrans, want, max)
	}
	snap := snapshotAssociation(a)
	if snap.SCTPError != nil || snap.SCTP.RTOInfo != want || snap.SCTP.AssociationMaxRetrans != max {
		t.Fatalf("snapshot=%+v", snap)
	}
}

func TestSCTPRTOSelectedRefusalKeepsListenerServing(t *testing.T) {
	ips := requireLoopbacks(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := mcSGPConfig()
	cfg.RTOInfo = &sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 400 * time.Millisecond, Max: 3500 * time.Millisecond}
	bad := snapshotAssociationConfig(cfg)
	bad.RTOInfo = &sctp.RTOInfo{Min: 4 * time.Second}
	lc := NewListenerConfig(cfg)
	calls := 0
	lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
		calls++
		if calls == 1 {
			return bad, nil
		}
		return cfg, nil
	}
	ln, err := listenSGP("m3ua", mcAddr(0, ips[0]), lc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	type result struct {
		a   *Association
		err error
	}
	accepted := make(chan result, 1)
	go func() { a, e := ln.Accept(ctx); accepted <- result{a, e} }()
	raw, err := sctp.Dial(ctx, "sctp4", mcAddr(0, ips[1]), ln.Addr().(*sctp.Addr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	refused := <-accepted
	if refused.a != nil {
		_ = refused.a.Close()
		t.Fatal("invalid selection returned an association")
	}
	if !errors.Is(refused.err, syscall.EINVAL) {
		t.Fatalf("selection error=%v want go-sctp EINVAL", refused.err)
	}
	go func() { a, e := ln.Accept(ctx); accepted <- result{a, e} }()
	a, err := dialASP(ctx, "m3ua", mcAddr(0, ips[1]), ln.Addr().(*sctp.Addr), mcASPConfig(mcASPPointCode))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	next := <-accepted
	if next.err != nil {
		t.Fatalf("next accept: %v", next.err)
	}
	defer func() { _ = next.a.Close() }()
}

func TestSCTPAssociationMaxRetransSetupOrdering(t *testing.T) {
	ips := requireLoopbacks(t, 3)
	for _, max := range []uint16{5, 4} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ln, err := sctp.Listen("sctp4", mcAddr(0, ips[:2]...))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			type result struct {
				c *sctp.Conn
				e error
			}
			accepted := make(chan result, 1)
			go func() { c, e := ln.AcceptSCTP(); accepted <- result{c, e} }()
			raw, err := sctp.Dial(ctx, "sctp4", mcAddr(0, ips[2]), ln.Addr().(*sctp.Addr))
			if err != nil {
				t.Fatal(err)
			}
			peer := <-accepted
			if peer.e != nil {
				t.Fatal(peer.e)
			}
			defer func() { _ = peer.c.Close() }()
			cfg := mcASPConfig(mcASPPointCode)
			pathMax := uint16(2)
			cfg.PathParams = &sctp.PathParams{PathMaxRetrans: &pathMax}
			cfg.AssociationMaxRetrans = max
			a := newAssociation(RoleASP, cfg)
			a.sctpConn = raw
			defer func() { _ = a.Close() }()
			peers, err := raw.PeerAddrs()
			if err != nil {
				t.Fatal(err)
			}
			if len(peers.IPs) != 2 {
				t.Fatalf("need two paths, got %v", peers)
			}
			err = a.setUpSocket()
			if max == 5 {
				if !errors.Is(err, syscall.EINVAL) {
					t.Fatalf("max 5 over path sum 4: %v", err)
				}
				if _, e := raw.Status(); !errors.Is(e, net.ErrClosed) {
					t.Fatalf("transport survived setup failure: %v", e)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				got, e := a.SCTPAssociationMaxRetrans()
				if e != nil || got != 4 {
					t.Fatalf("max=%d error=%v", got, e)
				}
				if e := a.SetSCTPAssociationMaxRetrans(5); !errors.Is(e, syscall.EINVAL) {
					t.Fatalf("runtime max above sum: %v", e)
				}
			}
		})
	}
}
