// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

// These tests open SCTP sockets and require four loopback addresses.
func TestSCTPPathSocketManagement(t *testing.T) {
	ips := requireLoopbacks(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type subscribed struct {
		a      *Association
		events <-chan *ManagementIndication
		err    error
	}
	seen := make(chan subscribed, 2)
	configure := func(cfg *AssociationConfig, interval time.Duration, max uint16, pf sctp.PFExposure) {
		enabled := true
		threshold := uint16(1)
		cfg.SCTPConfig.PathParams = &sctp.PathParams{Heartbeat: &enabled, HeartbeatInterval: &interval, PathMaxRetrans: &max}
		cfg.SCTPConfig.PathThresholds = &sctp.PathThresholds{PathMaxRetrans: &max, PFThreshold: &threshold}
		cfg.SCTPConfig.PFExposure = pf
		cfg.Subscribe = func(a *Association) {
			ch := a.ManagementIndications()
			// Subscribe runs before either M3UA reader or opening ASP procedure.
			var checkErr error
			peer, err := a.sctpConn.PeerAddrs()
			if err != nil {
				checkErr = err
			} else {
				for _, ip := range append([]netip.Addr{{}}, peer.IPs...) {
					p, e := a.SCTPPathParams(ip)
					if e != nil {
						checkErr = e
						break
					}
					th, e := a.SCTPPathThresholds(ip)
					if e != nil {
						checkErr = e
						break
					}
					if p.HeartbeatInterval == nil || *p.HeartbeatInterval != interval || p.Heartbeat == nil || !*p.Heartbeat || p.PathMaxRetrans == nil || *p.PathMaxRetrans != max || th.PathMaxRetrans == nil || *th.PathMaxRetrans != max || th.PFThreshold == nil || *th.PFThreshold != threshold {
						checkErr = fmt.Errorf("%v: params=%+v thresholds=%+v", ip, p, th)
						break
					}
				}
				exposure, e := a.sctpConn.PFExposure()
				if e != nil {
					checkErr = e
				} else if exposure != pf {
					checkErr = fmt.Errorf("PFExposure=%v, want %v", exposure, pf)
				}
			}
			seen <- subscribed{a, ch, checkErr}
		}
	}
	sgpConfig, aspConfig := mcSGPConfig(), mcASPConfig(mcASPPointCode)
	configure(sgpConfig, 700*time.Millisecond, 4, sctp.PFExposeDisabled)
	configure(aspConfig, 900*time.Millisecond, 3, sctp.PFExposeEnabled)
	ln, err := listenSGP("m3ua", mcAddr(0, ips[:2]...), NewListenerConfig(sgpConfig))
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
	cli, err := dialASP(ctx, "m3ua", mcAddr(0, ips[2:]...), ln.Addr().(*sctp.Addr), aspConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	srvResult := <-accepted
	if srvResult.err != nil {
		t.Fatal(srvResult.err)
	}
	srv := srvResult.a
	defer func() { _ = srv.Close() }()
	var cliEvents, srvEvents <-chan *ManagementIndication
	for range 2 {
		sub := <-seen
		if sub.err != nil {
			t.Fatalf("pre-handshake socket setup: %v", sub.err)
		}
		if sub.a == cli {
			cliEvents = sub.events
		} else {
			srvEvents = sub.events
		}
	}
	for _, a := range []*Association{cli, srv} {
		st, e := a.AssociationStatus()
		if e != nil {
			t.Fatal(e)
		}
		if len(st.LocalAddrs.IPs) != 2 || len(st.Paths) != 2 {
			t.Fatalf("live addresses/paths = %+v", st)
		}
		peer, e := a.sctpConn.PeerAddrs()
		if e != nil {
			t.Fatal(e)
		}
		for i, path := range st.Paths {
			if path.Addr.Addr() != peer.IPs[i] || path.MTU == 0 {
				t.Fatalf("path %d = %+v, peers=%v", i, path, peer)
			}
		}
		if !slices.Contains(peer.IPs, st.Primary.Addr.Addr()) {
			t.Fatalf("primary=%v peers=%v", st.Primary, peer)
		}
		snapshot := snapshotAssociation(a)
		if snapshot.SCTPError != nil || len(snapshot.SCTP.Paths) != 2 || len(snapshot.SCTP.LocalAddrs.IPs) != 2 {
			t.Fatalf("Endpoint snapshot=%+v", snapshot)
		}
	}
	initial, err := cli.AssociationStatus()
	if err != nil {
		t.Fatal(err)
	}
	var target netip.Addr
	for _, p := range initial.Paths {
		if p.Addr != initial.Primary.Addr {
			target = p.Addr.Addr()
		}
	}
	// Wait for Linux to verify the secondary path before selecting it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, e := cli.AssociationStatus()
		if e != nil {
			t.Fatal(e)
		}
		confirmed := false
		for _, p := range st.Paths {
			if p.Addr.Addr() == target && p.State == sctp.PathActive {
				confirmed = true
			}
		}
		if confirmed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("secondary never became active")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Distinct payloads let a packet capture identify DATA before/after selection.
	for i, ip := range []netip.Addr{initial.Primary.Addr.Addr(), target} {
		if err := cli.SetSCTPPrimaryAddr(ip); err != nil {
			t.Fatal(err)
		}
		st, e := cli.AssociationStatus()
		if e != nil {
			t.Fatal(e)
		}
		if st.Primary.Addr.Addr() != ip {
			t.Fatalf("primary=%v, want %v", st.Primary.Addr, ip)
		}
		payload := fmt.Sprintf("path-management-primary-%d", i)
		if _, e := mcWrite(cli, mcASPPointCode, mcSGPPointCode, payload); e != nil {
			t.Fatal(e)
		}
		msg, e := srv.ReadData(ctx)
		if e != nil || string(msg.ProtocolData.Data) != payload {
			t.Fatalf("DATA=%v error=%v", msg, e)
		}
		t.Logf("primary phase %d: %v payload %s", i, ip, payload)
	}
	before, err := cli.SCTPPathParams(initial.Primary.Addr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	on := true
	interval := 1300 * time.Millisecond
	if err := cli.SetSCTPPathParams(target, &sctp.PathParams{Heartbeat: &on, HeartbeatInterval: &interval}); err != nil {
		t.Fatal(err)
	}
	for _, ip := range []netip.Addr{initial.Primary.Addr.Addr(), target} {
		p, e := cli.SCTPPathParams(ip)
		if e != nil {
			t.Fatal(e)
		}
		want := *before.HeartbeatInterval
		if ip == target {
			want = interval
		}
		if *p.HeartbeatInterval != want {
			t.Fatalf("%v heartbeat=%v want=%v", ip, *p.HeartbeatInterval, want)
		}
	}
	newPF := uint16(0)
	if err := cli.SetSCTPPathThresholds(target, &sctp.PathThresholds{PFThreshold: &newPF}); err != nil {
		t.Fatal(err)
	}
	th, err := cli.SCTPPathThresholds(target)
	if err != nil || th.PFThreshold == nil || *th.PFThreshold != 0 {
		t.Fatalf("runtime thresholds=%+v error=%v", th, err)
	}
	for _, call := range []func() error{
		func() error { return cli.SetSCTPPrimaryAddr(netip.Addr{}) },
		func() error { return cli.SetSCTPPrimaryAddr(netip.MustParseAddr("192.0.2.99")) },
		func() error { return cli.SetSCTPPathParams(target, nil) },
		func() error { return cli.SetSCTPPathThresholds(target, nil) },
	} {
		if e := call(); !errors.Is(e, syscall.EINVAL) {
			t.Fatalf("validation error=%v want EINVAL", e)
		}
	}
	t.Logf("platform %s/%s", runtime.GOOS, runtime.GOARCH)
	confirmed, madePrimary := 0, 0
	eventDeadline := time.NewTimer(3 * time.Second)
	defer eventDeadline.Stop()
	for madePrimary < 2 {
		select {
		case ind, open := <-cliEvents:
			if !open {
				t.Fatal("association closed before both primary-change indications")
			}
			if ind.Kind == ManagementSCTPStatus {
				t.Logf("Linux path indication: %s", ind.Description)
				switch ind.PeerAddrChange.State {
				case sctp.AddrConfirmed:
					confirmed++
				case sctp.AddrMadePrimary:
					madePrimary++
				}
			}
		case <-eventDeadline.C:
			t.Fatalf("timed out waiting for primary changes: confirmed=%d made-primary=%d", confirmed, madePrimary)
		}
	}
	// Linux reports each local SetPrimaryAddr with SCTP_ADDR_MADE_PRIM.
	// CONFIRMED may already have arrived during SCTP establishment, before
	// the management subscription exists.
	if madePrimary != 2 {
		t.Fatalf("Linux loopback events: confirmed=%d made-primary=%d", confirmed, madePrimary)
	}
	for draining := true; draining; {
		select {
		case ind, open := <-srvEvents:
			if !open {
				t.Fatal("accepted association closed during path management")
			}
			if ind.Kind == ManagementSCTPStatus {
				t.Logf("accepted Linux path indication: %s", ind.Description)
			}
		default:
			draining = false
		}
	}
	if err := cli.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.SCTPPathParams(target); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed path params=%v", err)
	}
	if _, err := cli.AssociationStatus(); err == nil {
		t.Fatal("closed status succeeded")
	}
}

func TestSCTPPathSocketSetupFailureClosesTransport(t *testing.T) {
	ips := requireLoopbacks(t, 2)
	for _, tt := range []struct {
		name      string
		configure func(*AssociationConfig)
	}{
		{"params", func(c *AssociationConfig) { zero := uint16(0); c.PathParams = &sctp.PathParams{PathMaxRetrans: &zero} }},
		{"thresholds", func(c *AssociationConfig) {
			zero := uint16(0)
			c.PathThresholds = &sctp.PathThresholds{PathMaxRetrans: &zero}
		}},
		{"PF exposure", func(c *AssociationConfig) { c.PFExposure = sctp.PFExposure(99) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ln, err := sctp.Listen("sctp4", mcAddr(0, ips[0]))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			accepted := make(chan *sctp.Conn, 1)
			go func() { peer, _ := ln.AcceptSCTP(); accepted <- peer }()
			raw, err := sctp.Dial(ctx, "sctp4", mcAddr(0, ips[1]), ln.Addr().(*sctp.Addr))
			if err != nil {
				t.Fatal(err)
			}
			peer := <-accepted
			if peer == nil {
				t.Fatal("Accept failed")
			}
			defer func() { _ = peer.Close() }()
			cfg := mcASPConfig(mcASPPointCode)
			tt.configure(cfg)
			a := newAssociation(RoleASP, cfg)
			a.sctpConn = raw
			defer func() { _ = a.Close() }()
			if err := a.setUpSocket(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("setup error=%v want EINVAL", err)
			}
			if _, err := raw.Status(); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("transport survived setup failure: %v", err)
			}
		})
	}
}
