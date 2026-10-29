// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"os"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// Dial used to hand the association attempt to the kernel's defaults and wait
// for however long they took. Measured against a peer that never answers, with
// net.sctp.max_init_retransmits at its default of 8 and the RTO doubling from 3
// seconds to a 60 second ceiling, that is nine INIT chunks over 342 seconds
// before the call returns.
//
// An application that wants to control its own retry cadence cannot: by the
// time Dial comes back the situation it was reacting to is six minutes old. The
// attempt is now a single bounded one — the caller loops if it wants to retry —
// and the socket is released before Dial returns, on every path.
//
// The one-shot contract uses a setup RTO.Initial beyond the attempt budget.
// go-sctp restores the association's RTO.Initial once established, and Dial
// leaves the configured RTO.Min and RTO.Max unchanged. Linux paths retain the
// setup RTO until their first RTT measurement, retransmission timeout or
// association restart; see Dial and https://github.com/sctp/lksctp-tools/issues/74.

func TestOneShotSCTPDialPolicy(t *testing.T) {
	tests := []struct {
		name        string
		timeout     time.Duration
		wantInitial time.Duration
	}{
		{name: "zero", timeout: 0, wantInitial: time.Millisecond},
		{name: "negative", timeout: -time.Second, wantInitial: time.Millisecond},
		{name: "whole millisecond", timeout: time.Millisecond, wantInitial: 1001 * time.Millisecond},
		{name: "round up", timeout: time.Millisecond + time.Nanosecond, wantInitial: 1002 * time.Millisecond},
		{name: "below saturation", timeout: time.Duration(uint64(^uint32(0))-rtoMarginMillis-1) * time.Millisecond, wantInitial: time.Duration(uint64(^uint32(0))-1) * time.Millisecond},
		{name: "at saturation", timeout: time.Duration(uint64(^uint32(0))-rtoMarginMillis) * time.Millisecond, wantInitial: time.Duration(^uint32(0)) * time.Millisecond},
		{name: "sub-millisecond", timeout: time.Nanosecond, wantInitial: 1001 * time.Millisecond},
		{name: "default budget", timeout: DefaultInitTimeout, wantInitial: 6000 * time.Millisecond},
		{name: "longer than default kernel max", timeout: 2 * time.Minute, wantInitial: 121000 * time.Millisecond},
		{name: "saturates", timeout: time.Duration(1<<63 - 1), wantInitial: time.Duration(^uint32(0)) * time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := oneShotSCTPDialPolicy(test.timeout)
			if policy.init.OutStreams != sctpStreams || policy.init.MaxInStreams != sctpStreams {
				t.Errorf("streams = %d out, %d in, want %d each", policy.init.OutStreams, policy.init.MaxInStreams, sctpStreams)
			}
			if policy.init.MaxAttempts != 1 {
				t.Errorf("MaxAttempts = %d, want 1", policy.init.MaxAttempts)
			}
			if policy.init.MaxInitTimeout != time.Millisecond {
				t.Errorf("MaxInitTimeout = %d, want 1", policy.init.MaxInitTimeout)
			}
			if policy.setupRTOInitial != test.wantInitial {
				t.Errorf("setup RTO.Initial = %v, want %v", policy.setupRTOInitial, test.wantInitial)
			}
			cfg := policy.socketConfig(&restartWatcher{})
			if cfg.RTOInfo != nil || cfg.SetupRTOInitial != policy.setupRTOInitial {
				t.Errorf("Config RTOInfo = %+v, SetupRTOInitial = %v; want nil, %v", cfg.RTOInfo, cfg.SetupRTOInitial, policy.setupRTOInitial)
			}
			if policy.abandon != sctp.AbandonQuiet {
				t.Errorf("abandon policy = %v, want AbandonQuiet", policy.abandon)
			}
			if !reflect.DeepEqual(policy.events, associationEvents()) {
				t.Errorf("events = %v, want %v subscribed before connect", policy.events, associationEvents())
			}
		})
	}
}

type captureAbandonPolicyDialer struct {
	err   error
	calls int
}

func (d *captureAbandonPolicyDialer) Dial(
	_ context.Context,
	_ string,
	_, _ *sctp.Addr,
) (*sctp.Conn, error) {
	d.calls++
	return nil, d.err
}

func TestOneShotSCTPDialPolicyUsesQuietAbandon(t *testing.T) {
	dialer := &captureAbandonPolicyDialer{err: context.Canceled}
	policy := oneShotSCTPDialPolicy(DefaultInitTimeout)

	_, err := policy.dialContext(context.Background(), dialer, "sctp4", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dial error = %v, want context.Canceled", err)
	}
	if dialer.calls != 1 {
		t.Fatalf("DialContextWithAbandonPolicy calls = %d, want 1", dialer.calls)
	}
	if policy.socketConfig(&restartWatcher{}).AbandonPolicy != sctp.AbandonQuiet {
		t.Fatal("Dial Config must use AbandonQuiet")
	}
}

func TestOneShotSCTPDialPolicyRTOStaysPastBudget(t *testing.T) {
	tests := []time.Duration{
		time.Millisecond,
		time.Second,
		DefaultInitTimeout,
		30 * time.Second,
		2 * time.Minute,
		time.Hour,
		24 * time.Hour,
	}

	for _, timeout := range tests {
		t.Run(timeout.String(), func(t *testing.T) {
			policy := oneShotSCTPDialPolicy(timeout)
			rto := policy.setupRTOInitial
			budgetMillis := uint64(timeout / time.Millisecond)
			if timeout%time.Millisecond != 0 {
				budgetMillis++
			}
			if uint64(rto/time.Millisecond) <= budgetMillis {
				t.Fatalf("RTO Initial = %dms, not beyond InitTimeout %dms", rto/time.Millisecond, budgetMillis)
			}
			cfg := policy.socketConfig(&restartWatcher{})
			if cfg.RTOInfo != nil || cfg.SetupRTOInitial != rto {
				t.Errorf("Config RTOInfo = %+v, SetupRTOInitial = %v; want nil, %v", cfg.RTOInfo, cfg.SetupRTOInitial, rto)
			}
		})
	}
}

// blackholeAddr is in TEST-NET-1 (RFC 5737), reserved for documentation.
// These tests require traffic to it to be silently dropped, for example in a
// privileged Linux container with: ip route add blackhole 192.0.2.0/24
//
// An ABORT answering the INIT gives ECONNREFUSED. An ICMP Protocol Unreachable
// answering the INIT in COOKIE-WAIT is handled like an ABORT (RFC 9260 Section
// 10, ICMP8); Linux reports ENOPROTOOPT. ICMP Port Unreachable is ignored
// (RFC 9260 Section 10, ICMP3). An immediate refusal exercises no timeout.
func blackholeAddr(t *testing.T) *sctp.Addr {
	t.Helper()

	addr, err := sctp.ResolveAddr("sctp4", "192.0.2.1:2905")
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

// skipUnlessBlackholed skips when the address answers instead of staying
// silent, so a refused connect is never mistaken for a bounded timeout.
func skipUnlessBlackholed(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		return
	}
	if isSCTPUnsupported(err) {
		t.Skipf("skipping socket-backed test: %v", err)
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOPROTOOPT) {
		t.Skip("TEST-NET-1 must silently drop traffic; use a privileged Linux container with: ip route add blackhole 192.0.2.0/24")
	}
}

func dialCfg(initTimeout time.Duration) *AssociationConfig {
	cfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1})
	cfg.InitTimeout = initTimeout
	return cfg
}

// The attempt is bounded by InitTimeout, not by the kernel's retransmission
// budget.
func TestDialGivesUpOnItsOwnTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: dials a peer that never answers")
	}

	ctx := context.Background()
	start := time.Now()
	conn, err := dialASP(ctx, "m3ua4", nil, blackholeAddr(t), dialCfg(2*time.Second))
	elapsed := time.Since(start)

	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("Dial reported success against an address that never answers")
	}
	skipUnlessBlackholed(t, err)
	if !errors.Is(err, ErrInitTimeout) {
		t.Errorf("Dial error = %v, want ErrInitTimeout", err)
	}
	// Generously bounded: the point is that it is nothing like the kernel's
	// 342 seconds, and that it tracks the configured timeout.
	if elapsed > 6*time.Second {
		t.Errorf("Dial took %v with a 2s InitTimeout; it is still waiting on the kernel's budget", elapsed)
	}
}

// A cancelled context must abandon the attempt promptly, and report the
// cancellation rather than a timeout.
func TestDialAbandonsOnContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: dials a peer that never answers")
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	conn, err := dialASP(ctx, "m3ua4", nil, blackholeAddr(t), dialCfg(30*time.Second))
	elapsed := time.Since(start)

	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("Dial reported success after its context was cancelled")
	}
	skipUnlessBlackholed(t, err)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Dial error = %v, want context.Canceled", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Dial took %v to notice cancellation", elapsed)
	}
}

// A context already cancelled must not start an attempt at all.
func TestDialWithACancelledContextDoesNotAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fdBaseline, haveFDs := openDescriptors()
	start := time.Now()
	conn, err := dialASP(ctx, "m3ua4", nil, blackholeAddr(t), dialCfg(30*time.Second))
	elapsed := time.Since(start)

	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Dial error = %v, want context.Canceled", err)
	}
	if elapsed > time.Second {
		t.Errorf("Dial took %v on an already-cancelled context; it performed the connect anyway", elapsed)
	}
	if haveFDs {
		got, _ := openDescriptors()
		if got != fdBaseline {
			t.Errorf("open descriptors = %d after already-cancelled Dial, baseline %d: an attempt was opened",
				got, fdBaseline)
		}
	}
}

// openDescriptors counts this process's open file descriptors, or reports 0 and
// false where that cannot be read.
//
// The count is only meaningful after at least one dial has already happened:
// Go's runtime opens its netpoll epoll instance and a wake eventfd lazily on
// first network use, so the very first attempt in a process legitimately adds
// two descriptors that are never released and are not sockets. Taking the
// baseline after the warm-up dial is what makes the comparison honest.
func openDescriptors() (int, bool) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false // not Linux, or procfs is not mounted
	}
	return len(entries), true
}

// The point of bounding the attempt is that a caller can run its own retry
// loop. Repeating it must not accumulate goroutines or descriptors, since a
// reconnect loop runs for the life of the process.
//
// Both halves of that sentence are asserted. The descriptor half was not, for
// as long as this test existed: it counted goroutines alone while its own
// comment promised descriptors too, so a Dial that released every goroutine and
// leaked the socket on each abandoned attempt would have passed. That is the
// more consequential of the two failures, since a process holding a descriptor
// per attempt hits its rlimit and can no longer dial at all.
func TestRepeatedFailedDialsDoNotLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: several full dial attempts")
	}

	ctx := context.Background()
	cfg := dialCfg(time.Second)

	// One warm-up so lazily started machinery is not counted.
	if conn, err := dialASP(ctx, "m3ua4", nil, blackholeAddr(t), cfg); err == nil {
		_ = conn.Close()
		t.Skip("the blackhole address answered; this environment cannot run the test")
	} else {
		skipUnlessBlackholed(t, err)
	}

	settle()
	baseline := runtime.NumGoroutine()
	fdBaseline, haveFDs := openDescriptors()

	const rounds = 5
	for i := 0; i < rounds; i++ {
		conn, err := dialASP(ctx, "m3ua4", nil, blackholeAddr(t), cfg)
		if err == nil {
			_ = conn.Close()
			t.Fatal("the blackhole address answered mid-test")
		}
	}

	// The abandoned attempts are released asynchronously, bounded by the
	// kernel's INIT budget, so allow them time to finish before counting.
	time.Sleep(12 * time.Second)
	settle()

	if got := runtime.NumGoroutine(); got > baseline+4 {
		t.Errorf("goroutines = %d after %d failed dials, baseline %d: each attempt is leaking",
			got, rounds, baseline)
	}

	if haveFDs {
		// The baseline was taken after the warm-up dial, so the runtime's
		// netpoll descriptors are already in it and any growth here is one
		// abandoned attempt's socket surviving. A small allowance absorbs
		// unrelated runtime activity without admitting a per-round leak, which
		// over five rounds would show as five.
		got, _ := openDescriptors()
		if got > fdBaseline+2 {
			t.Errorf("open descriptors = %d after %d failed dials, baseline %d: "+
				"an abandoned attempt is not releasing its socket",
				got, rounds, fdBaseline)
		}
	}
}

// Repeated START/STOP cycles cancel in-flight attempts rather than waiting for
// InitTimeout. They must release the same resources as timeout paths, because
// this is the path an application uses for rapid shutdown.
func TestRepeatedCancelledDialsDoNotLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: several cancellable dial attempts")
	}

	// One warm-up so lazily started machinery is not counted.
	warmCtx, warmCancel := context.WithCancel(context.Background())
	warmCancel()
	conn, err := dialASP(warmCtx, "m3ua4", nil, blackholeAddr(t), dialCfg(30*time.Second))
	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("warm-up Dial error = %v, want context.Canceled", err)
	}

	settle()
	baseline := runtime.NumGoroutine()
	fdBaseline, haveFDs := openDescriptors()

	const rounds = 20
	for i := 0; i < rounds; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(10*time.Millisecond, cancel)

		start := time.Now()
		conn, err := dialASP(ctx, "m3ua4", nil, blackholeAddr(t), dialCfg(30*time.Second))
		elapsed := time.Since(start)
		cancel()
		if conn != nil {
			_ = conn.Close()
		}
		if err == nil {
			t.Fatal("Dial reported success against an address that never answers")
		}
		skipUnlessBlackholed(t, err)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("round %d Dial error = %v, want context.Canceled", i, err)
		}
		if elapsed > time.Second {
			t.Fatalf("round %d Dial took %v after cancellation", i, elapsed)
		}
	}

	settle()
	if got := runtime.NumGoroutine(); got > baseline+4 {
		t.Errorf("goroutines = %d after %d cancelled dials, baseline %d: each attempt is leaking",
			got, rounds, baseline)
	}

	if haveFDs {
		got, _ := openDescriptors()
		if got > fdBaseline+2 {
			t.Errorf("open descriptors = %d after %d cancelled dials, baseline %d: "+
				"an abandoned attempt is not releasing its socket",
				got, rounds, fdBaseline)
		}
	}
}

// InitTimeout and EstablishTimeout are separate budgets: the first bounds
// getting the association up, the second bounds the M3UA handshake on top of it.
// A peer that completes the association and then says nothing must be caught by
// the second, not left to the first.
func TestEstablishTimeoutBoundsTheM3UAHandshake(t *testing.T) {
	ctx := context.Background()

	// Accepts the association, answers no M3UA at all.
	peer := newRawPeer(t, 3170, func(messages.M3UA) messages.M3UA { return nil })

	cfg := dialCfg(5 * time.Second)
	cfg.EstablishTimeout = time.Second

	laddr, err := sctp.ResolveAddr("sctp", "127.0.0.1:3170")
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	conn, err := dialASP(ctx, "m3ua", laddr, peer.addr, cfg)
	elapsed := time.Since(start)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("Dial succeeded against a peer that never completed the M3UA handshake")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("Dial error = %v, want ErrTimeout", err)
	}
	if elapsed > 4*time.Second {
		t.Errorf("Dial took %v with a 1s EstablishTimeout; the handshake budget is not being honoured", elapsed)
	}
}

func TestDialHandshakeError(t *testing.T) {
	for _, test := range []struct {
		name             string
		endpointClosed   bool
		operationCause   error
		associationCause error
		want             error
	}{
		{"Endpoint close before callback", true, nil, ErrAssociationClosed, ErrEndpointClosed},
		{"Endpoint close after callback", true, ErrEndpointClosed, ErrAssociationClosed, ErrEndpointClosed},
		{"Endpoint close after caller cancellation", true, context.Canceled, context.Canceled, ErrEndpointClosed},
		{"operation Endpoint cause", false, ErrEndpointClosed, ErrAssociationClosed, ErrEndpointClosed},
		{"association close", false, nil, ErrAssociationClosed, ErrAssociationClosed},
		{"caller cancellation", false, context.Canceled, context.Canceled, context.Canceled},
		{"handshake failure", false, nil, ErrTimeout, ErrTimeout},
		{"unspecified failure", false, nil, nil, ErrFailedToEstablish},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })
			// Keep the contexts independent to model the interval before
			// operationContext's AfterFunc propagates Endpoint cancellation.
			operationCtx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			if test.operationCause != nil {
				cancel(test.operationCause)
			}
			if test.endpointClosed {
				if err := endpoint.Close(); err != nil {
					t.Fatal(err)
				}
			}
			association := newAssociation(RoleASP, NewAssociationConfig())
			if err := association.closeWith(test.associationCause); err != nil {
				t.Fatal(err)
			}
			if err := endpoint.dialHandshakeError(operationCtx, association); !errors.Is(err, test.want) {
				t.Fatalf("Dial handshake error = %v, want %v", err, test.want)
			}
		})
	}
}
