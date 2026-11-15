package m3ua

import (
	"runtime"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func waitStatusCommit(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("status commit barrier timed out")
		}
		runtime.Gosched()
	}
}

func TestOfferedStatusReceivedClearSurvivesDelayedCanonicalProjection(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	e.ssnm.validity = time.Hour // No automation worker in this socket-free fixture.
	a, b := peers["sg-a/sgp-a1"], peers["sg-a/sgp-a2"]
	const pc, other = uint32(0x123456), uint32(0x123457)
	for _, dpc := range []uint32{pc, other} {
		applyASPSCON(t, a, 7, 1, dpc, 0, params.NewCongestionIndications(2))
		applyASPSCON(t, b, 7, 1, dpc, 0, params.NewCongestionIndications(0))
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, pc), true)
	requireOfferedStatus(t, offerStatusAt(t, e, other), true)
	r := e.aspRoutes
	r.mu.Lock()
	sequence := r.sequence
	r.mu.Unlock()
	a.destinations.mu.Lock()
	b.destinations.mu.Lock()
	aLocked, bLocked, routeLocked := true, true, false
	defer func() {
		if aLocked {
			a.destinations.mu.Unlock()
		}
		if bLocked {
			b.destinations.mu.Unlock()
		}
		if routeLocked {
			r.mu.Unlock()
		}
	}()
	handle := func(c *Association, level uint8) <-chan error {
		done := make(chan error, 1)
		go func() {
			done <- c.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(0, pc), nil, params.NewCongestionIndications(level), nil))
		}()
		return done
	}
	clear, onset := handle(a, 0), handle(b, 2)
	// Both handlers finish legacy projection while canonical congestion is still 2.
	waitStatusCommit(t, func() bool { r.mu.RLock(); defer r.mu.RUnlock(); return r.sequence >= sequence+2 })
	r.mu.Lock()
	routeLocked = true
	a.destinations.mu.Unlock()
	aLocked = false
	partition := canonicalSSNMPartition("sg-a", "as-core")
	as := a.staticASKeyForRoutingContext(1, false)
	waitStatusCommit(t, func() bool { return !e.ssnm.destinationKnowledge(partition, pc, as).congested })
	b.destinations.mu.Unlock()
	bLocked = false
	waitStatusCommit(t, func() bool { return e.ssnm.destinationKnowledge(partition, pc, as).congested })
	// Canonical history was 2,0,2 before either post-commit projection can run.
	r.mu.Unlock()
	routeLocked = false
	for _, done := range []<-chan error{clear, onset} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("handler projection did not complete")
		}
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, other), false)
	requireOfferedStatus(t, offerStatusAt(t, e, pc), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, pc), offer == 9)
	}
}
