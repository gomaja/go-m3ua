// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// A bounded exchange must produce a bounded number of messages.
//
// The state machine publishes a state after every message, and entering a state
// runs its entry action — for a client, "send ASP Up" on ASP-DOWN and "send ASP
// Active" on ASP-INACTIVE. When a handler restated its current state to mean
// "hold", the entry action used to run again, so any peer whose answer the
// handler rejected closed a loop: reject, restate, re-send, be answered,
// reject. On the wire that was 1797 M3UA messages and 934 Errors inside a
// second, from a plain Routing Context mismatch between an ASP and its SGP.
//
// A single unit test pins one such peer. This drives the whole ASPSM/ASPTM
// exchange against a peer whose answers are chosen by the fuzzer, and asserts
// only that the conversation terminates: no reachable combination of Acks,
// Routing Contexts and traffic modes may keep the two ends talking forever.

// settleBudget is how many steps the exchange gets to reach a fixed point. A
// correct handshake settles in well under ten; anything that reaches this is
// not converging.
const settleBudget = 200

// peerReply models the far end. It answers a request with the Ack the protocol
// says answers it, parameterised by the fuzzer so the Ack may disagree with
// what the ASP asked for — a foreign Routing Context, an incompatible traffic
// mode — which is what a misconfigured peer really does.
func peerReply(msg messages.M3UA, rc *params.Param, tm *params.Param, silent bool) messages.M3UA {
	if silent {
		return nil
	}
	switch msg.(type) {
	case *messages.AspUp:
		return messages.NewAspUpAck(nil, nil)
	case *messages.AspActive:
		return messages.NewAspActiveAck(tm, rc, nil)
	case *messages.AspInactive:
		return messages.NewAspInactiveAck(rc, nil)
	case *messages.AspDown:
		return messages.NewAspDownAck(nil)
	default:
		// Errors, Acks and everything else are not answered: a peer that
		// answered an Error with an Error would be the amplifier itself, and
		// that is a separate, already-tested rule.
		return nil
	}
}

// runExchange drives conn until nothing further is pending, or the budget runs
// out. It reports the number of steps taken and everything the Conn sent.
//
// It is the in-process equivalent of monitor(): pull a published state and
// apply it, and when nothing is pending, hand the peer's answer to the
// dispatcher.
func runExchange(t *testing.T, conn *Conn, sent *[]messages.M3UA, rc, tm *params.Param, silent bool) (steps int, settled bool) {
	t.Helper()

	ctx := context.Background()
	answered := 0

	// Bootstrap exactly as Dial and Accept do.
	conn.stateChan <- StateAspDown

	for steps = 0; steps < settleBudget; steps++ {
		// Errors are reported to monitor(), which logs them; drain so a full
		// channel cannot be mistaken for a settled exchange.
		select {
		case <-conn.errChan:
			continue
		default:
		}

		select {
		case st := <-conn.stateChan:
			// Entry actions run here, and may write through signalWriter.
			_ = conn.handleStateUpdate(st)
			continue
		default:
		}

		// Nothing pending: let the peer answer the next thing we sent.
		if answered < len(*sent) {
			req := (*sent)[answered]
			answered++
			if reply := peerReply(req, rc, tm, silent); reply != nil {
				conn.handleSignals(ctx, reply)
			}
			continue
		}

		return steps, true
	}
	return steps, false
}

// FuzzExchangeAlwaysSettles sweeps the peer's possible answers and requires the
// exchange to terminate for every one of them.
func FuzzExchangeAlwaysSettles(f *testing.F) {
	for _, seed := range []struct {
		rcs    []uint32
		tm     uint32
		silent bool
		server bool
	}{
		{[]uint32{1, 2}, params.TrafficModeLoadshare, false, false}, // agreeing peer
		{[]uint32{99}, params.TrafficModeLoadshare, false, false},   // foreign RC: the storm
		{[]uint32{1, 2}, params.TrafficModeOverride, false, false},  // incompatible mode
		{nil, params.TrafficModeLoadshare, false, false},            // RC omitted
		{[]uint32{1, 2}, params.TrafficModeLoadshare, true, false},  // silent peer
		{[]uint32{99}, params.TrafficModeBroadcast, false, true},    // same, at the SGP
	} {
		rcBytes := make([]byte, 0, len(seed.rcs)*4)
		for _, v := range seed.rcs {
			rcBytes = append(rcBytes, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		}
		f.Add(rcBytes, seed.tm, seed.silent, seed.server)
	}

	f.Fuzz(func(t *testing.T, rcData []byte, tmValue uint32, silent, server bool) {
		// A Routing Context parameter is a list of 32-bit values; anything else
		// exercises the decoder's own guards, which other targets cover.
		if len(rcData) > 64 {
			return
		}

		m := modeClient
		if server {
			m = modeServer
		}
		conn, sent := newTestConn(t, StateAspDown, m)

		rc := params.NewParam(int(params.RoutingContext), rcData)
		tm := params.NewTrafficModeType(tmValue)

		steps, settled := runExchange(t, conn, sent, rc, tm, silent)
		if !settled {
			t.Fatalf("exchange did not settle in %d steps: %d messages sent (%v). "+
				"A bounded conversation must produce a bounded number of messages",
				steps, len(*sent), typeNames(*sent))
		}

		// Settling is the load-bearing property, but a settled exchange that
		// emitted hundreds of messages would still be an amplifier.
		if len(*sent) > 16 {
			t.Fatalf("exchange settled after %d steps but sent %d messages (%v)",
				steps, len(*sent), typeNames(*sent))
		}
	})
}

// The agreeing case must still complete the handshake, so the fuzz target above
// is not satisfied by a state machine that simply does nothing.
func TestExchangeWithAgreeingPeerReachesActive(t *testing.T) {
	conn, sent := newTestConn(t, StateAspDown, modeClient)

	steps, settled := runExchange(t, conn, sent,
		params.NewRoutingContext(1, 2),
		params.NewTrafficModeType(params.TrafficModeLoadshare), false)
	if !settled {
		t.Fatalf("agreeing exchange did not settle in %d steps", steps)
	}

	if got := conn.State(); got != StateAspActive {
		t.Errorf("state = %v after a complete handshake, want %v (sent %v)", got, StateAspActive, typeNames(*sent))
	}
	want := []string{"ASP Up", "ASP Active"}
	if got := typeNames(*sent); len(got) != len(want) {
		t.Errorf("sent %v, want exactly %v", got, want)
	}
}
