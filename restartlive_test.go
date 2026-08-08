// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// dropSCTPTeardown makes the local stack discard inbound association-teardown
// chunks, so an endpoint can be made to miss its peer going away.
//
// The drop is on INPUT rather than OUTPUT deliberately: an OUTPUT rule discards
// the chunks before they reach the wire at all, which would make the teardown
// unobservable to a capture as well as to the peer.
//
// It returns a removal function, which the caller must run before closing the
// associations it made: while the rule is up the test's own teardown is dropped
// too, so the endpoints sit in shutdown until the guard timer fires and the
// listening port stays bound. Left to a t.Cleanup that runs after the deferred
// Closes, that made a second consecutive run fail to bind -- which is a property
// of this rule, not of the library. The removal is idempotent and also
// registered as a cleanup, so an early t.Fatal cannot leave the rule installed.
//
// Reports false when the rule cannot be installed, which is the ordinary case
// without --privileged.
func dropSCTPTeardown(t *testing.T) (remove func(), ok bool) {
	t.Helper()

	rule := []string{"-p", "sctp", "--chunk-types", "any",
		"ABORT,SHUTDOWN,SHUTDOWN_ACK,SHUTDOWN_COMPLETE", "-j", "DROP"}
	if out, err := exec.Command("iptables",
		append([]string{"-I", "INPUT", "1"}, rule...)...).CombinedOutput(); err != nil {
		t.Logf("cannot install the SCTP teardown drop: %v: %s", err, out)
		return func() {}, false
	}

	var once sync.Once
	remove = func() {
		once.Do(func() {
			_ = exec.Command("iptables", append([]string{"-D", "INPUT"}, rule...)...).Run()
		})
	}
	t.Cleanup(remove)
	return remove, true
}

// RFC 4666 Section 1.6.3 lists M-SCTP_RESTART: "M3UA informs LM that an SCTP
// restart indication has been received." RFC 9260 Section 5.2.4 defines what is
// being reported — a peer that re-establishes the same association without
// tearing the old one down keeps the association at this end, reset, with
// everything the peer held gone.
//
// The unit tests drive the watcher with a synthetic sctp_assoc_change and prove
// routing and filtering; TestAssociationEventsAreSubscribedOnALiveAssociation
// proves the subscription. Neither proves the kernel ever emits SCTP_RESTART,
// which is the one link in the chain this library does not control. This closes
// that gap by inducing a real restart: the SGP is made to miss the ASP's
// teardown, and the ASP then re-establishes from the same five-tuple, so the
// INIT lands on a live TCB and the receiver runs the restart procedure.
//
// Needs --privileged for the iptables rule, and skips without it.
func TestSCTPRestartIsReportedFromARealRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3231

	srvCfg := NewServerConfig(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 1)
	srvAddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen("m3ua", srvAddr, NewListenerConfig(srvCfg))
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	restoreTeardown, ok := dropSCTPTeardown(t)
	if !ok {
		t.Skip("skipping: the SGP cannot be made to miss the ASP's teardown " +
			"without iptables; run the container with --privileged")
	}

	accepted := make(chan *Conn, 4)
	go func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	cliCfg := NewClientConfig(&HeartbeatInfo{Enabled: false},
		0x11111111, 0x22222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 1)
	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}

	first, err := Dial(ctx, "m3ua", laddr, srvAddr, cliCfg)
	if err != nil {
		t.Fatal(err)
	}
	var sgp *Conn
	select {
	case sgp = <-accepted:
	case <-time.After(15 * time.Second):
		t.Fatal("the SGP never accepted the first association")
	}
	if sgp.assocID.Load() == 0 {
		t.Fatal("the accepted association has no id; the restart cannot be routed to it")
	}
	// Accept returns only after ASP-ACTIVE, so all opening state indications are
	// already queued. Remove them to make every edge observed below attributable
	// to the SCTP restart rather than to the initial handshake.
drainInitialStates:
	for {
		select {
		case _, ok := <-sgp.StateChanges():
			if !ok {
				t.Fatal("the SGP state indication channel closed before the restart was induced")
			}
		case <-sgp.Done():
			t.Fatal("the SGP association closed before the restart was induced")
		default:
			break drainInitialStates
		}
	}

	// The ASP goes away and the SGP does not hear about it.
	_ = first.Close()
	time.Sleep(500 * time.Millisecond)
	select {
	case <-sgp.Done():
		t.Fatal("the SGP saw the teardown; the drop rule did not take effect, so " +
			"what follows would be an ordinary new association rather than a restart")
	default:
	}

	// The ASP comes back on the same five-tuple. This is the restart.
	second, err := Dial(ctx, "m3ua", laddr, srvAddr, cliCfg)
	if err != nil {
		t.Fatalf("the ASP could not re-establish: %v", err)
	}
	defer func() { _ = second.Close() }()
	// Registered after that Close, so it runs before it: deferred calls are
	// LIFO. The rule has to come off ahead of every teardown this test performs,
	// including on the failure paths below, or the associations sit in shutdown
	// and the listening port is still bound when the next run tries to bind it.
	defer restoreTeardown()

	deadline := time.After(20 * time.Second)
	sawRestart, sawDown, sawRecovered := false, false, false
	for !sawRestart || !sawDown || !sawRecovered {
		select {
		case ind, ok := <-sgp.ManagementIndications():
			if !ok {
				t.Fatal("the SGP's indication channel closed before the restart was reported")
			}
			if ind.Kind != ManagementSCTPRestart {
				continue // Notify and Error indications also arrive here.
			}
			if ind.Description == "" {
				t.Error("the restart indication carries no description")
			}
			sawRestart = true
		case state, ok := <-sgp.StateChanges():
			if !ok {
				t.Fatal("the SGP's state indication channel closed during restart recovery")
			}
			switch state {
			case StateAspDown:
				sawDown = true
			case StateAspActive:
				if sawDown {
					sawRecovered = true
				}
			}
		case c := <-accepted:
			t.Fatalf("the SGP accepted a new association (id %d) instead of "+
				"seeing a restart of id %d; the re-INIT did not land on the "+
				"existing TCB and this test is not exercising a restart",
				c.assocID.Load(), sgp.assocID.Load())
		case <-deadline:
			t.Fatalf("real SCTP restart did not complete the M3UA procedure within 20s: "+
				"M-SCTP_RESTART=%v ASP-DOWN=%v recovered ASP-ACTIVE=%v",
				sawRestart, sawDown, sawRecovered)
		}
	}

	if got := sgp.State(); got != StateAspActive {
		t.Errorf("SGP state after restart recovery = %v, want %v", got, StateAspActive)
	}
}
