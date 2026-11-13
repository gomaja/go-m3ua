// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func overflowAssociation(t *testing.T) (*Association, *[]messages.M3UA) {
	t.Helper()
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	c.dataChan = make(chan *DataMessage, 2)
	deliver(t, c, 1, inboundData(1, "queued"))
	deliver(t, c, 1, inboundData(1, "queued"))
	return c, sent
}

func handleOverflowReports(t *testing.T, c *Association) {
	t.Helper()
	for {
		select {
		case err := <-c.errChan:
			if e := c.handleErrors(err); e != nil {
				t.Fatal(e)
			}
		default:
			return
		}
	}
}

func TestOverflowSCONCarriesTheReceivedScope(t *testing.T) {
	c, sent := overflowAssociation(t)
	for _, rc := range []uint32{1, 2, 1} {
		deliver(t, c, 1, inboundData(rc, "discarded"))
		handleOverflowReports(t, c)
	}
	if len(*sent) != 2 {
		t.Fatalf("sent %d SCONs, want one for each of two scopes", len(*sent))
	}
	for i, m := range *sent {
		s, ok := m.(*messages.SignallingCongestion)
		if !ok {
			t.Fatalf("sent %T, want SCON", m)
		}
		if s.NetworkAppearance == nil || s.NetworkAppearance.NetworkAppearance() != 7 {
			t.Errorf("SCON Network Appearance = %v, want received 7", s.NetworkAppearance)
		}
		if s.RoutingContext == nil || !reflect.DeepEqual(s.RoutingContext.RoutingContexts(), []uint32{uint32(i + 1)}) {
			t.Errorf("SCON Routing Context = %v, want received %d", s.RoutingContext, i+1)
		}
		if s.AffectedPointCode == nil || !reflect.DeepEqual(s.AffectedPointCode.AffectedPointCodes(), []uint32{0x222222}) {
			t.Errorf("SCON affected destination = %v", s.AffectedPointCode)
		}
		if s.ConcernedDestination == nil || s.ConcernedDestination.ConcernedDestination() != 0x111111 {
			t.Errorf("SCON Concerned Destination = %v, want triggering OPC", s.ConcernedDestination)
		}
		if s.CongestionIndications != nil {
			t.Error("international SCON includes Congestion Indications")
		}
	}
}

func TestOverflowEpisodeEndsAfterSustainedDrain(t *testing.T) {
	c, _ := overflowAssociation(t)
	deliver(t, c, 1, inboundData(1, "discarded"))
	for range 2 {
		if _, err := c.ReadData(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if !c.DataQueueStats().Congested {
		t.Fatal("episode ended on first drain, before the 1 s dwell")
	}
	deadline := time.Now().Add(2 * time.Second)
	for c.DataQueueStats().Congested && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if c.DataQueueStats().Congested {
		t.Fatal("episode never ended after sustained drain with no further DATA")
	}
}

func TestOverflowEpisodeStaysWhileOccupancyStaysHigh(t *testing.T) {
	c, _ := overflowAssociation(t)
	deliver(t, c, 1, inboundData(1, "discarded"))
	time.Sleep(1100 * time.Millisecond)
	if !c.DataQueueStats().Congested {
		t.Fatal("episode ended above 50% capacity")
	}
	if _, err := c.ReadData(context.Background()); err != nil {
		t.Fatal(err)
	}
	deliver(t, c, 1, inboundData(1, "refill"))
	time.Sleep(1100 * time.Millisecond)
	if !c.DataQueueStats().Congested {
		t.Fatal("successful enqueue ended an episode while the queue was full")
	}
}

func TestOverflowAtSGPDoesNotReportSS7DestinationCongestion(t *testing.T) {
	c, sent := overflowAssociation(t)
	c.role = RoleSGP
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	deliver(t, c, 1, inboundData(1, "discarded"))
	handleOverflowReports(t, c)
	if len(*sent) != 0 {
		t.Fatalf("SGP sent %d SCONs for local queue exhaustion without SS7 congestion evidence", len(*sent))
	}
}

func nextQueueCongestion(t *testing.T, c *Association) DataQueueCongestionIndication {
	t.Helper()
	select {
	case m := <-c.ManagementIndications():
		if m == nil || m.Kind != ManagementDataQueueCongestion || m.DataQueueCongestion == nil {
			t.Fatalf("management indication = %+v, want queue transition", m)
		}
		return *m.DataQueueCongestion
	case <-time.After(2 * time.Second):
		t.Fatal("missing management queue transition")
	}
	return DataQueueCongestionIndication{}
}

func TestOverflowManagementSurvivesFailedSCON(t *testing.T) {
	c, _ := overflowAssociation(t)
	c.signalWriter = func(messages.M3UA) (int, error) { return 0, context.DeadlineExceeded }
	for _, rc := range []uint32{1, 2} {
		deliver(t, c, 1, inboundData(rc, "discarded"))
		handleOverflowReports(t, c)
	}
	for _, rc := range []uint32{1, 2} {
		ind := nextQueueCongestion(t, c)
		if !ind.Congested || ind.TrackingLost || !reflect.DeepEqual(ind.Scope.RoutingContexts, []uint32{rc}) || ind.OriginatingPointCode != 0x111111 || ind.DestinationPointCode != 0x222222 {
			t.Fatalf("onset = %+v", ind)
		}
		// Mutation of the delivered scope cannot corrupt the retained end report.
		ind.Scope.RoutingContexts[0] = 99
	}
	for range 2 {
		if _, err := c.ReadData(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[uint32]bool{}
	for range 2 {
		ind := nextQueueCongestion(t, c)
		if ind.Congested || ind.TrackingLost {
			t.Fatalf("end = %+v", ind)
		}
		seen[ind.Scope.RoutingContexts[0]] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("end scopes = %v", seen)
	}
}

func TestOverflowScopesRetainPresenceAndOriginator(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 0)
	c.noteRoutingContextsAcked(params.NewRoutingContext(0))
	c.dataChan = make(chan *DataMessage, 1)
	deliver(t, c, 1, messages.NewData(nil, nil, params.NewProtocolData(1, 2, 3, 0, 0, 1, nil), nil))
	type wireCase struct {
		na, rc   *params.Param
		opc, dpc uint32
	}
	cases := []wireCase{
		{nil, nil, 1, 2}, {params.NewNetworkAppearance(0), nil, 1, 2},
		{nil, params.NewRoutingContext(0), 1, 2}, {nil, nil, 3, 2}, {nil, nil, 1, 4},
	}
	for _, tc := range cases {
		deliver(t, c, 1, messages.NewData(tc.na, tc.rc, params.NewProtocolData(tc.opc, tc.dpc, 3, 0, 0, 1, nil), nil))
		if firstErr(c) == nil {
			t.Fatal("distinct received scope/origin/destination was coalesced")
		}
		ind := nextQueueCongestion(t, c)
		if ind.Scope.NetworkAppearanceSet != (tc.na != nil) || ind.Scope.RoutingContextSet != (tc.rc != nil) || ind.OriginatingPointCode != tc.opc || ind.DestinationPointCode != tc.dpc {
			t.Fatalf("onset = %+v", ind)
		}
		s := c.overflowSCON(&DataQueueOverflowError{Scope: ind.Scope, OriginatingPointCode: tc.opc, DestinationPointCode: tc.dpc})
		if (s.NetworkAppearance != nil) != (tc.na != nil) || (s.RoutingContext != nil) != (tc.rc != nil) {
			t.Fatalf("SCON lost parameter presence: %+v", s)
		}
	}
}

func TestOverflowScopeLimitReportsContinuityLoss(t *testing.T) {
	c, _ := overflowAssociation(t)
	for i := 0; i < dataOverflowScopeLimit+2; i++ {
		deliver(t, c, 1, messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(uint32(i), 2, 3, 0, 0, 1, nil), nil))
		if i < dataOverflowScopeLimit {
			if firstErr(c) == nil {
				t.Fatal("missing tracked onset")
			}
			if ind := nextQueueCongestion(t, c); ind.TrackingLost {
				t.Fatal("premature scope tracking loss")
			}
		}
	}
	if ind := nextQueueCongestion(t, c); !ind.TrackingLost {
		t.Fatal("scope tracking loss not reported")
	}
	if got := c.DataQueueStats().ScopeTrackingLosses; got != 2 {
		t.Fatalf("scope tracking losses=%d, want 2", got)
	}
	c.muDataOverflow.Lock()
	tracked := len(c.dataOverflowScopes)
	c.muDataOverflow.Unlock()
	if tracked != dataOverflowScopeLimit {
		t.Fatalf("retained %d scopes", tracked)
	}
	select {
	case m := <-c.ManagementIndications():
		t.Fatalf("repeated loss flooded management: %+v", m)
	default:
	}
}

func TestOverflowDrainThresholdAndDwellBoundaries(t *testing.T) {
	for _, capacity := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			c, _ := overflowAssociation(t)
			// This test models occupancy observations under the production lock.
			c.dataChan = make(chan *DataMessage, capacity)
			deliver(t, c, 1, inboundData(1, "queued"))
			for len(c.dataChan) < capacity {
				deliver(t, c, 1, inboundData(1, "queued"))
			}
			deliver(t, c, 1, inboundData(1, "discarded"))
			start := time.Now().Add(time.Hour)
			c.muDataOverflow.Lock()
			defer c.muDataOverflow.Unlock()
			low := capacity / 2
			c.observeDataQueueLocked(start, low)
			c.observeDataQueueLocked(start.Add(time.Second-time.Nanosecond), low)
			if !c.dataOverflow.Load() {
				t.Fatal("ended before 1 s")
			}
			c.observeDataQueueLocked(start.Add(time.Second), low+1)
			if !c.dataOverflow.Load() || !c.dataOverflowLowSince.IsZero() {
				t.Fatal("high occupancy did not cancel dwell")
			}
			c.observeDataQueueLocked(start.Add(2*time.Second), low)
			c.observeDataQueueLocked(start.Add(3*time.Second), low)
			if c.dataOverflow.Load() {
				t.Fatal("did not end at threshold after 1 s")
			}
		})
	}
}

func TestOverflowIPSPOnsetOmitsConcernedDestination(t *testing.T) {
	c, sent := overflowAssociation(t)
	c.role = RoleIPSP
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	deliver(t, c, 1, inboundData(1, "discarded"))
	handleOverflowReports(t, c)
	if len(*sent) != 1 {
		t.Fatalf("IPSP sent %d SCONs, want 1", len(*sent))
	}
	s := (*sent)[0].(*messages.SignallingCongestion)
	if s.ConcernedDestination != nil || s.CongestionIndications != nil {
		t.Fatalf("IPSP SCON has ASP-to-SGP or national-only parameters: %+v", s)
	}
}

func TestOverflowSCONSendBackpressureDoesNotCloseAssociation(t *testing.T) {
	c, _ := overflowAssociation(t)
	c.signalWriter = nil
	transport := newFullSendBuffer()
	c.controlTransport = transport
	c.transportCloser = transport.close
	c.cfg.ControlWriteTimeout = 20 * time.Millisecond
	deliver(t, c, 1, inboundData(1, "discarded"))
	handleOverflowReports(t, c)
	select {
	case <-c.Done():
		t.Fatalf("optional overflow SCON closed association: %v", c.Err())
	default:
	}
	ind := nextQueueCongestion(t, c)
	if !ind.Congested {
		t.Fatal("missing onset after failed socket send")
	}
	for range 2 {
		if _, err := c.ReadData(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if ind = nextQueueCongestion(t, c); ind.Congested {
		t.Fatal("missing end after failed socket send")
	}
}

func TestOverflowEpisodeCanRestartAfterRecovery(t *testing.T) {
	c, _ := overflowAssociation(t)
	for episode := 0; episode < 2; episode++ {
		deliver(t, c, 1, inboundData(1, "discarded"))
		if firstErr(c) == nil {
			t.Fatalf("episode %d missing onset", episode)
		}
		if ind := nextQueueCongestion(t, c); !ind.Congested {
			t.Fatalf("episode %d onset=%+v", episode, ind)
		}
		for range 2 {
			if _, err := c.ReadData(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if ind := nextQueueCongestion(t, c); ind.Congested {
			t.Fatalf("episode %d end=%+v", episode, ind)
		}
		if episode == 0 {
			for range 2 {
				deliver(t, c, 1, inboundData(1, "refill"))
			}
		}
	}
	if !waitFor(func() bool { c.muDataOverflow.Lock(); defer c.muDataOverflow.Unlock(); return !c.dataOverflowWorker }, time.Second) {
		t.Fatal("idle episode worker did not stop")
	}
}

func TestOverflowDiscardInterruptsDrainDwell(t *testing.T) {
	c, _ := overflowAssociation(t)
	deliver(t, c, 1, inboundData(1, "discarded"))
	_ = firstErr(c)
	c.muDataOverflow.Lock()
	c.dataOverflowLowSince = time.Now().Add(-dataOverflowDrainDwell)
	c.muDataOverflow.Unlock()
	deliver(t, c, 1, inboundData(1, "discarded again"))
	c.muDataOverflow.Lock()
	defer c.muDataOverflow.Unlock()
	if !c.dataOverflowLowSince.IsZero() {
		t.Fatal("discard did not interrupt drain dwell")
	}
	if len(c.dataOverflowScopes) != 1 {
		t.Fatal("repeated discard was not coalesced")
	}
}

func TestOverflowWorkerStopsWhenAssociationCloses(t *testing.T) {
	c, _ := overflowAssociation(t)
	deliver(t, c, 1, inboundData(1, "discarded"))
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { c.muDataOverflow.Lock(); defer c.muDataOverflow.Unlock(); return !c.dataOverflowWorker }, time.Second) {
		t.Fatal("overflow worker survived association close")
	}
}
