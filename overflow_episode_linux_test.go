// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestOverflowSCONSocketScopesAndSustainedDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	receivedSCON := make(chan *messages.SignallingCongestion, 4)
	peer := newRawPeer(t, 0, func(m messages.M3UA) messages.M3UA {
		if scon, ok := m.(*messages.SignallingCongestion); ok {
			// rawPeer reuses its receive buffer. Own the decoded parameters before
			// the next read overwrites them, so this checks each received message.
			raw, err := scon.MarshalBinary()
			if err != nil {
				t.Error(err)
				return nil
			}
			owned, err := messages.ParseSignallingCongestion(raw)
			if err != nil {
				t.Error(err)
				return nil
			}
			receivedSCON <- owned
		}
		return handshakeOnly(m)
	})
	cfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1, 2})
	cfg.DataQueueSize = 2
	cfg.Subscribe = func(c *Association) { c.ManagementIndications() }
	c, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.ln.Addr().(*sctp.Addr), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if !waitFor(func() bool { return c.routingContextAcked(1) && c.routingContextAcked(2) }, 2*time.Second) {
		t.Fatal("both contexts did not activate")
	}
	send := func(rc uint32) {
		t.Helper()
		raw, e := inboundData(rc, "synthetic queue overload").MarshalBinary()
		if e != nil {
			t.Fatal(e)
		}
		peer.sendOn(t, raw, 1)
	}
	// The ASP accepts received NA=7 even though the coordinated local NA is 0.
	// Every message uses one ordered DATA stream, so the first two fill the queue.
	send(1)
	send(2)
	send(1)
	send(2)
	send(1)
	send(2)
	if !waitFor(func() bool { return c.DataQueueStats().Discarded == 4 }, 2*time.Second) {
		t.Fatalf("queue=%+v", c.DataQueueStats())
	}
	onsets := map[uint32]bool{}
	for range 2 {
		ind := nextQueueCongestion(t, c)
		if !ind.Congested || ind.TrackingLost || !ind.Scope.NetworkAppearanceSet || ind.Scope.NetworkAppearance != 7 {
			t.Fatalf("onset=%+v", ind)
		}
		onsets[ind.Scope.RoutingContexts[0]] = true
		t.Logf("management onset: %+v", ind)
	}
	if !onsets[1] || !onsets[2] {
		t.Fatalf("onsets=%v", onsets)
	}
	if !waitFor(func() bool { return peer.count("Signalling Congestion") == 2 }, 2*time.Second) {
		t.Fatalf("SCON count=%d", peer.count("Signalling Congestion"))
	}
	sconScopes := map[uint32]bool{}
	for range 2 {
		var s *messages.SignallingCongestion
		select {
		case s = <-receivedSCON:
		case <-ctx.Done():
			t.Fatal("missing owned SCON")
		}
		if s.NetworkAppearance == nil || s.NetworkAppearance.NetworkAppearance() != 7 || s.RoutingContext == nil || len(s.RoutingContext.RoutingContexts()) != 1 || s.AffectedPointCode == nil || !reflect.DeepEqual(s.AffectedPointCode.AffectedPointCodes(), []uint32{0x222222}) || s.ConcernedDestination == nil || s.ConcernedDestination.ConcernedDestination() != 0x111111 || s.CongestionIndications != nil {
			t.Fatalf("SCON=%+v", s)
		}
		sconScopes[s.RoutingContext.RoutingContexts()[0]] = true
	}
	if !sconScopes[1] || !sconScopes[2] {
		t.Fatalf("SCON scopes=%v", sconScopes)
	}
	drained := time.Now()
	for range 2 {
		if _, err = c.ReadData(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !c.DataQueueStats().Congested {
		t.Fatal("episode ended on first dequeue")
	}
	ends := map[uint32]bool{}
	for range 2 {
		ind := nextQueueCongestion(t, c)
		if ind.Congested || time.Since(drained) < time.Second {
			t.Fatalf("premature end=%+v after %s", ind, time.Since(drained))
		}
		ends[ind.Scope.RoutingContexts[0]] = true
		t.Logf("management end after %s: %+v", time.Since(drained), ind)
	}
	if !ends[1] || !ends[2] || c.DataQueueStats().Congested {
		t.Fatalf("ends=%v queue=%+v", ends, c.DataQueueStats())
	}
	// With international SCON there is no level-zero wire clear. Leave the
	// connection alive for several checks so a spurious clear reaches the peer.
	time.Sleep(3 * dataOverflowCheckInterval)
	if got := peer.count("Signalling Congestion"); got != 2 {
		t.Fatalf("SCON count after local end=%d, want 2", got)
	}
}

// Hold the real association's writer gate while its receive queue recovers.
// This models delayed admission, not SCTP network/backlog congestion.
func TestOverflowSCONSocketDropsRecoveredOnset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peer := newRawPeer(t, 0, handshakeOnly)
	cfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1, 2})
	cfg.DataQueueSize = 2
	cfg.Subscribe = func(c *Association) { c.ManagementIndications() }
	c, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.ln.Addr().(*sctp.Addr), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if !waitFor(func() bool { return c.routingContextAcked(1) && c.routingContextAcked(2) }, 2*time.Second) {
		t.Fatal("both contexts did not activate")
	}
	c.sendMu.Lock()
	var release sync.Once
	unlock := func() { release.Do(c.sendMu.Unlock) }
	defer unlock()
	for range 3 {
		raw, e := inboundData(1, "synthetic pending onset").MarshalBinary()
		if e != nil {
			t.Fatal(e)
		}
		peer.sendOn(t, raw, 1)
	}
	if !waitFor(func() bool { return c.DataQueueStats().Discarded == 1 }, time.Second) {
		t.Fatalf("queue=%+v", c.DataQueueStats())
	}
	if !nextQueueCongestion(t, c).Congested {
		t.Fatal("missing onset")
	}
	waitForOverflowWriter(t)
	for range 2 {
		if _, err = c.ReadData(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if nextQueueCongestion(t, c).Congested {
		t.Fatal("missing end")
	}
	unlock()
	if !waitFor(func() bool { return len(goroutinesBlockedIn("handleErrors")) == 0 }, time.Second) {
		t.Fatal("writer did not finish dropping expired onset")
	}
	time.Sleep(3 * dataOverflowCheckInterval)
	if got := peer.count("Signalling Congestion"); got != 0 {
		t.Fatalf("peer received %d obsolete SCONs after recovery", got)
	}
}
