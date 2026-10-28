// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func TestOneShotSCTPDialPolicySaturatedValueIsAccepted(t *testing.T) {
	policy := oneShotSCTPDialPolicy(time.Duration(1<<63 - 1))
	cfg := policy.socketConfig(&restartWatcher{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	peer := &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Port: 2905}
	// go-sctp validates Config before checking ctx, then returns before opening
	// a socket. An out-of-range setup value would return EINVAL instead.
	conn, err := cfg.Dial(ctx, "sctp4", nil, peer)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("cancelled Dial returned a connection")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("saturated setup RTO validation = %v, want context.Canceled", err)
	}
}

func TestDialRestoresAssociationRTO(t *testing.T) {
	readMillis := func(name string) time.Duration {
		t.Helper()
		b, err := os.ReadFile("/proc/sys/net/sctp/" + name)
		if os.IsNotExist(err) {
			t.Skipf("SCTP sysctl %s is unavailable: %v", name, err)
		}
		if err != nil {
			t.Fatal(err)
		}
		ms, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		return time.Duration(ms) * time.Millisecond
	}
	want := sctp.RTOInfo{
		Initial: readMillis("rto_initial"),
		Min:     readMillis("rto_min"),
		Max:     readMillis("rto_max"),
	}
	for i, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"default", 0},
		{"30s", 30 * time.Second},
		{"saturates", time.Duration(1<<63 - 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeout := tc.timeout
			if timeout == 0 {
				timeout = DefaultInitTimeout
			}
			setup := time.Duration(oneShotRTOMillisFor(timeout)) * time.Millisecond
			if want.Initial == setup {
				t.Skipf("namespace rto_initial equals setup RTO %v; restoration would be indistinguishable", setup)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			hb := &HeartbeatInfo{Enabled: false}
			client, server, err := setupConnHBWithInitTimeout(t, ctx, 29980+i, hb, hb, tc.timeout)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close(); _ = server.Close() }()
			for client.State() != StateASPActive || server.State() != StateASPActive {
				select {
				case <-ctx.Done():
					t.Fatalf("states = %v / %v: %v", client.State(), server.State(), ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			dialled, err := client.sctpConn.RTOInfo()
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := server.sctpConn.RTOInfo()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("setup = %v; dialled = %+v; accepted = %+v; namespace = %+v", setup, *dialled, *accepted, want)
			if *dialled != *accepted {
				t.Errorf("dialled RTOInfo = %+v, accepted = %+v", *dialled, *accepted)
			}
			if *dialled != want || *accepted != want {
				t.Errorf("dialled = %+v, accepted = %+v, want namespace RTOInfo %+v", *dialled, *accepted, want)
			}
			if dialled.Initial == setup {
				t.Errorf("established RTO.Initial still equals setup value %v", setup)
			}
		})
	}
}
