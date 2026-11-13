package m3ua

import (
	"context"
	"errors"
	"github.com/gomaja/go-sctp"
	"sync"
	"testing"
	"time"
)

func TestIPSPTypedCongestionWithdrawalOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	aEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, TransportCongestion: &TransportCongestionConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = aEndpoint.Close() })
	bEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, TransportCongestion: &TransportCongestionConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bEndpoint.Close() })
	cfgA := integrationIPSPDoubleExchangeConfig(IPSPASPSMExchangeDouble, true, ipspIntegrationTraffic(10, 11), ipspIntegrationTraffic(20, 22))
	cfgB := integrationIPSPDoubleExchangeConfig(IPSPASPSMExchangeDouble, true, ipspIntegrationTraffic(20, 22), ipspIntegrationTraffic(10, 11))
	cfgA.TAck, cfgB.TAck = time.Hour, time.Hour
	listener, err := bEndpoint.Listen("m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(cfgB))
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan associationResult, 1)
	go func() { c, e := listener.Accept(ctx); accepted <- associationResult{association: c, err: e} }()
	a, err := aEndpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.Addr), cfgA)
	if err != nil {
		t.Fatal(err)
	}
	var b *Association
	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatal(result.err)
		}
		b = result.association
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForIPSPState(t, a, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	waitForIPSPState(t, b, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	report := func(c *Association, na, rc, pc uint32, level uint8) error {
		return c.SignallingCongestion(SignallingCongestionRequest{Scope: WireScope{NetworkAppearance: na, NetworkAppearanceSet: true, RoutingContexts: []uint32{rc}, RoutingContextSet: true}, Destinations: []PointCodeRange{{PointCode: pc}}, CongestionLevel: level, CongestionLevelSet: true})
	}
	// Optional SCON declines a contended state transition. Retry the complete
	// positive admission under a deadline; the queued withdrawal below stays
	// a single attempt whose rejection and wire absence are checked strictly.
	sendPositive := func(c *Association, na, rc, pc uint32, level uint8) {
		t.Helper()
		var sendErr error
		settled := waitFor(func() bool {
			sendErr = report(c, na, rc, pc, level)
			return sendErr == nil || !errors.Is(sendErr, ErrNotEstablished)
		}, time.Second)
		if !settled || sendErr != nil {
			t.Fatalf("positive local-direction SCON: %v", sendErr)
		}
	}
	observed := func(c *Association, na, rc, pc uint32, level uint8) bool {
		s, ok := c.destinations.lookup(destinationKey{networkAppearance: na, networkAppearanceSet: true, routingContext: rc, routingContextSet: true, pointCode: pc})
		return ok && s.Congestion.Congested && s.Congestion.Level == level
	}
	sendPositive(a, 10, 11, 100, 1)
	if !waitFor(func() bool { return observed(b, 10, 11, 100, 1) }, time.Second) {
		t.Fatal("peer did not receive the positive local-direction SCON")
	}
	b.sendMu.Lock()
	var releaseB sync.Once
	unlockB := func() { releaseB.Do(b.sendMu.Unlock) }
	defer unlockB()
	if err := a.DeactivateRoutingContexts(11); err != nil {
		t.Fatal(err)
	}
	a.sendMu.Lock()
	var releaseA sync.Once
	unlockA := func() { releaseA.Do(a.sendMu.Unlock) }
	defer unlockA()
	written := make(chan error, 1)
	go func() { written <- report(a, 10, 11, 101, 2) }()
	if !waitFor(func() bool {
		if a.localIPSPSSNMDeliveryMu.TryLock() {
			a.localIPSPSSNMDeliveryMu.Unlock()
			return false
		}
		return true
	}, time.Second) {
		t.Fatal("typed SCON did not wait for its writer")
	}
	unlockB()
	if !waitFor(func() bool { return !a.routingContextAcked(11) }, time.Second) {
		t.Fatal("withdrawal did not commit")
	}
	unlockA()
	select {
	case err := <-written:
		if !errors.Is(err, ErrNotEstablished) {
			t.Errorf("withdrawn typed SCON: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForIPSPState(t, a, IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive})
	sendPositive(b, 20, 22, 102, 3)
	if !waitFor(func() bool { return observed(a, 20, 22, 102, 3) }, time.Second) {
		t.Fatal("independent direction did not remain usable")
	}
	t.Log("SCON wire witnesses: NA10/RC11/APC100/level1; NA20/RC22/APC102/level3; withdrawn APC101 must be absent")
}
