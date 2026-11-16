package m3ua

import (
	"runtime"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func TestOfferedStatusEpochResetSurvivesDelayedProjection(t *testing.T) {
	config := validASPConfig()
	useSignallingGateways(config, "sg-a")
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	a := peers["sg-a/sgp-a1"]
	m := e.transport
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	m.readStatus = nil
	m.read = func(*Association) (*sctp.SendBacklog, error) {
		return &sctp.SendBacklog{Charged: 95, Limit: 100}, nil
	}
	m.config.OnsetDwell = time.Nanosecond
	job := transportCongestionJob{a, a.ID(), a.Epoch()}
	sample := func() { now = now.Add(100 * time.Millisecond); m.sample(e, job) }
	sample()
	sample()
	if state := a.TransportCongestion(); !state.Known || state.Level != 3 {
		t.Fatalf("onset control=%+v", state)
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)

	// New-epoch samples may commit while routing projection is delayed. Keep
	// the production coalescing path, with no worker consuming its notification.
	e.mu.Lock()
	if m.started {
		e.mu.Unlock()
		t.Fatal("fixture unexpectedly started sampler")
	}
	m.started = true
	m.projection = make(chan struct{}, 1)
	e.mu.Unlock()
	routes := e.aspRoutes
	routes.mu.Lock()
	locked := true
	defer func() {
		if locked {
			routes.mu.Unlock()
		}
	}()
	previousEpoch := a.Epoch()
	resetDone := make(chan struct{})
	go func() {
		a.resetTransportCongestionEpoch()
		close(resetDone)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for a.Epoch() == previousEpoch {
		if time.Now().After(deadline) {
			t.Fatal("epoch reset did not commit before route projection")
		}
		runtime.Gosched()
	}
	job.epoch = a.Epoch()
	sample()
	sample()
	if state := a.TransportCongestion(); !state.Known || state.Level != 3 || state.Epoch != job.epoch {
		t.Fatalf("new epoch congestion control=%+v", state)
	}
	routes.mu.Unlock()
	locked = false
	select {
	case <-resetDone:
	case <-time.After(2 * time.Second):
		t.Fatal("epoch projection did not finish after route unlock")
	}
	drainMTPIndications(e.MTPIndications())
	// The reset's projection sees the same positive level as before, but the
	// old transport lifetime must not retain its offered-message interval.
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}

func TestOfferedStatusEpochResetWithoutDetectorRetiresInterval(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare)
	a := peers["sg-a/sgp-a1"]
	if a.transportSnapshot.Load() != nil {
		t.Fatal("fixture has a transport detector contribution")
	}
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	a.resetTransportCongestionEpoch()
	if a.transportSnapshot.Load() != nil {
		t.Fatal("epoch reset invented a transport detector contribution")
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
}
