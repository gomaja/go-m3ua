package m3ua

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestTransportCongestionDwellRequiresContinuousSamples(t *testing.T) {
	for _, tc := range []struct {
		name    string
		level   uint8
		charged int
		want    uint8
		dwell   time.Duration
	}{
		{"onset", 0, 90, 3, 300 * time.Millisecond},
		{"abatement", 3, 0, 0, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := congestionTestConfig(t)
			start := time.Unix(100, 0)
			d := transportCongestionDetector{state: TransportCongestionState{Level: tc.level}}
			congestionObserve(&d, cfg, start, tc.charged, 100)
			resume := start.Add(5 * time.Second)
			if got := congestionObserve(&d, cfg, resume, tc.charged, 100); got.Level != tc.level {
				t.Fatalf("five-second sampling gap completed %s dwell: level=%d want=%d", tc.name, got.Level, tc.level)
			}
			for elapsed := cfg.Interval; elapsed < tc.dwell; elapsed += cfg.Interval {
				if got := congestionObserve(&d, cfg, resume.Add(elapsed), tc.charged, 100); got.Level != tc.level {
					t.Fatalf("gap reused pending %s dwell after %s: %+v", tc.name, elapsed, got)
				}
			}
			if got := congestionObserve(&d, cfg, resume.Add(tc.dwell), tc.charged, 100); got.Level != tc.want {
				t.Fatalf("fresh continuous %s dwell: level=%d want=%d", tc.name, got.Level, tc.want)
			}
		})
	}
}

func TestTransportCongestionCoverageBoundFollowsInterval(t *testing.T) {
	for _, interval := range []time.Duration{20 * time.Millisecond, 100 * time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) {
			cfg := congestionTestConfig(t)
			cfg.Interval, cfg.OnsetDwell = interval, 3*interval
			start := time.Time{}
			bound := interval + interval/2
			d := transportCongestionDetector{}
			congestionObserve(&d, cfg, start, 90, 100)
			congestionObserve(&d, cfg, start.Add(bound), 90, 100)
			if got := congestionObserve(&d, cfg, start.Add(2*bound), 90, 100); got.Level != 3 {
				t.Fatalf("continuous samples at the gap bound did not complete dwell: %+v", got)
			}
			d = transportCongestionDetector{}
			congestionObserve(&d, cfg, start, 90, 100)
			congestionObserve(&d, cfg, start.Add(bound+time.Nanosecond), 90, 100)
			if got := congestionObserve(&d, cfg, start.Add(2*(bound+time.Nanosecond)), 90, 100); got.Level != 0 {
				t.Fatalf("samples beyond the gap bound completed dwell: %+v", got)
			}
		})
	}
	cfg := congestionTestConfig(t)
	cfg.Interval = time.Duration(1<<63 - 1)
	d := transportCongestionDetector{}
	start := time.Unix(100, 0)
	congestionObserve(&d, cfg, start, 90, 100)
	if got := congestionObserve(&d, cfg, start.Add(cfg.OnsetDwell), 90, 100); got.Level != 3 {
		t.Fatalf("large interval overflowed the coverage bound: %+v", got)
	}
	d = transportCongestionDetector{}
	congestionObserve(&d, cfg, start, 90, 100)
	deadline := start.Add(cfg.Interval).Add(cfg.Interval / 2)
	if got := congestionObserve(&d, cfg, deadline.Add(time.Nanosecond), 90, 100); got.Level != 0 {
		t.Fatalf("large sampling gap was hidden by duration saturation: %+v", got)
	}
}

func TestTransportCongestionMissedSlotRestartsPendingDwell(t *testing.T) {
	for _, initial := range []uint8{0, 3} {
		t.Run(string(rune('0'+initial)), func(t *testing.T) {
			e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			a, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
			if !e.trackAssociation(a) {
				t.Fatal("track")
			}
			state := TransportCongestionState{Association: a.ID(), Epoch: a.Epoch(), Level: initial, Known: true}
			a.transportSnapshot.Store(&state)
			charged, want, dwell := 90, uint8(3), 300*time.Millisecond
			if initial == 3 {
				charged, want, dwell = 0, 0, time.Second
			}
			now := time.Unix(100, 0)
			start := now
			e.transport.now = func() time.Time { return now }
			e.transport.read = func(*Association) (*sctp.SendBacklog, error) {
				return &sctp.SendBacklog{Charged: charged, Limit: 100}, nil
			}
			job := transportCongestionJob{a, a.ID(), a.Epoch()}
			e.transport.sample(e, job)
			now = start.Add(100 * time.Millisecond)
			e.transport.sample(e, job)
			now = start.Add(150 * time.Millisecond)
			e.transport.missed(e, job)
			resume := start.Add(200 * time.Millisecond)
			for elapsed := time.Duration(0); elapsed < dwell; elapsed += 100 * time.Millisecond {
				now = resume.Add(elapsed)
				e.transport.sample(e, job)
				if got := a.TransportCongestion(); got.Level != initial {
					t.Fatalf("missed slot counted toward dwell: %+v", got)
				}
			}
			now = resume.Add(dwell)
			e.transport.sample(e, job)
			if got := a.TransportCongestion(); got.Level != want || got.MissedSamples != 1 {
				t.Fatalf("fresh dwell after missed slot: %+v", got)
			}
		})
	}
}

func TestTransportCongestionSnapshotDoesNotWaitForState(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	b, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	if !e.trackAssociation(a) || !e.trackAssociation(b) {
		t.Fatal("track associations")
	}
	// These identity-only connections never execute a socket operation.
	a.sctpConn, b.sctpConn = &sctp.Conn{}, &sctp.Conn{}
	a.transportReady.Store(true)
	b.transportReady.Store(true)
	t.Cleanup(func() {
		a.sctpConn, b.sctpConn = nil, nil
		_ = e.Close()
	})
	a.muState.Lock()
	result := make(chan []transportCongestionJob, 1)
	go func() { result <- e.transport.snapshot(e) }()
	var jobs []transportCongestionJob
	select {
	case jobs = <-result:
		a.muState.Unlock()
	case <-time.After(time.Second):
		a.muState.Unlock()
		<-result
		t.Fatal("membership snapshot waited for another association's state lock")
	}
	for _, job := range jobs {
		if job.association == b {
			return
		}
	}
	t.Fatal("the unlocked association disappeared from the sampling snapshot")
}

func TestTransportCongestionProjectionUsesDestinationAvailability(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	a, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 1)
	b, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 2)
	setAssociationTransport(t, e, a, 0)
	setAssociationTransport(t, e, b, 3)
	const x, y = uint32(0x123456), uint32(0x123457)
	applyASPDAVA(t, a, 7, 1, y, 0)
	applyASPDUNA(t, a, 7, 1, x, 0)
	for len(e.MTPIndications()) > 0 {
		<-e.MTPIndications()
	}
	applyASPDAVA(t, b, 7, 2, x, 0)
	applyASPDAVA(t, b, 7, 2, y, 0)
	result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(x, 1, nil)})
	if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Association != b.ID() {
		t.Fatalf("transfer did not use the available second AS: result=%+v err=%v", result, err)
	}
	destination := func(pc uint32) MTPDestination { return MTPDestination{MTPRoute: "sccp-a", PointCode: pc} }
	xStatus, _ := e.MTPDestinationStatus(destination(x))
	yStatus, _ := e.MTPDestinationStatus(destination(y))
	if !xStatus.Congested || xStatus.CongestionLevel != 3 {
		t.Fatalf("unavailable clear AS hid the usable AS pressure: %+v", xStatus)
	}
	if yStatus.Congested {
		t.Fatalf("destination-specific unavailability leaked to its healthy sibling: %+v", yStatus)
	}
	onset := false
	for len(e.MTPIndications()) > 0 {
		indication := <-e.MTPIndications()
		if indication.Kind == MTPStatusIndication && indication.Destination.Destination.PointCode == x && indication.Destination.Congested && indication.Destination.CongestionLevel == 3 {
			onset = true
		}
	}
	if !onset {
		t.Fatal("the sole usable congested route produced no MTP-STATUS onset")
	}
	applyASPDAVA(t, a, 7, 1, x, 0)
	if status, _ := e.MTPDestinationStatus(destination(x)); status.Congested {
		t.Fatalf("DAVA did not restore the clear preferred AS: %+v", status)
	}
	applyASPDUNA(t, a, 7, 1, x, 0)
	if status, _ := e.MTPDestinationStatus(destination(x)); !status.Congested || status.CongestionLevel != 3 {
		t.Fatalf("DUNA did not restore the congested fallback: %+v", status)
	}
	b.setState(StateASPInactive)
	if status, _ := e.MTPDestinationStatus(destination(x)); status.Congested {
		t.Fatalf("inactive fallback retained transport pressure: %+v", status)
	}
	b.setState(StateASPActive)
	if status, _ := e.MTPDestinationStatus(destination(x)); status.Congested {
		t.Fatalf("AS reactivation reused retired destination knowledge: %+v", status)
	}
	applyASPDAVA(t, b, 7, 2, x, 0)
	if status, _ := e.MTPDestinationStatus(destination(x)); !status.Congested {
		t.Fatalf("AS reactivation did not restore transport pressure: %+v", status)
	}
	b.resetTransportCongestionEpoch()
	if status, _ := e.MTPDestinationStatus(destination(x)); status.Congested || !status.TransportCongestionUnknown {
		t.Fatalf("restart did not retire the old transport contribution: %+v", status)
	}
}

func TestTransportCongestionProjectionTracksSecondaryASState(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 2)
	const pc = uint32(0x123456)
	applyASPDUNA(t, a, 7, 1, pc, 0)
	applyASPDAVA(t, a, 7, 2, pc, 0)
	setAssociationTransport(t, e, a, 3)
	destination := MTPDestination{MTPRoute: "sccp-a", PointCode: pc}
	if status, _ := e.MTPDestinationStatus(destination); !status.Congested {
		t.Fatalf("missing initial pressure: %+v", status)
	}
	a.noteRoutingContextsUnacked(params.NewRoutingContext(2))
	if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pc, 1, nil)}); !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("inactive secondary AS was still transferable: %v", err)
	}
	if status, _ := e.MTPDestinationStatus(destination); status.Congested {
		t.Fatalf("unchanged first AS hid the secondary AS becoming inactive: %+v", status)
	}
}

func TestTransportCongestionProjectionTracksCanonicalInvalidation(t *testing.T) {
	for _, invalidatePreferred := range []bool{false, true} {
		name := "fallback"
		if invalidatePreferred {
			name = "preferred"
		}
		t.Run(name, func(t *testing.T) {
			cfg := twoApplicationServerASPConfig()
			cfg.MaxAffectedPointCodesPerSSNM = 1
			e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: cfg})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
			a, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 1)
			b, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 2)
			const pc = uint32(0x123456)
			setAssociationTransport(t, e, a, 0)
			setAssociationTransport(t, e, b, 3)
			if invalidatePreferred {
				applyASPDAVA(t, a, 7, 1, pc, 0)
			} else {
				applyASPDUNA(t, a, 7, 1, pc, 0)
			}
			applyASPDAVA(t, b, 7, 2, pc, 0)
			target, rc := b, uint32(2)
			if invalidatePreferred {
				target, rc = a, 1
			}
			err = target.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(rc), params.NewAffectedPointCode(pc, pc+1), nil))
			if !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatalf("oversized report: %v", err)
			}
			result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pc, 1, nil)})
			if invalidatePreferred {
				if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Association != b.ID() {
					t.Fatalf("invalidation did not expose the available fallback: %+v %v", result, err)
				}
			} else if !errors.Is(err, ErrNoMTPRoute) {
				t.Fatalf("invalidated fallback remained transferable: %v", err)
			}
			status, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: pc})
			if status.Congested != invalidatePreferred {
				t.Fatalf("canonical invalidation left stale pressure: %+v", status)
			}
		})
	}
}

func TestTransportCongestionProjectionAcceptsPendingSiblingReports(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	a, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 1)
	b, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 2)
	const pc = uint32(0x123456)
	applyASPDAVA(t, a, 7, 1, pc, 0)
	applyASPDAVA(t, b, 7, 2, pc, 0)
	setAssociationTransport(t, e, a, 0)
	setAssociationTransport(t, e, b, 3)
	pending := attachActivatingSSNMAssociation(t, e, identity, 7, 1)
	applyASPDUNA(t, pending, 7, 1, pc, 0)
	result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pc, 1, nil)})
	if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Association != b.ID() {
		t.Fatalf("pending sibling's DUNA did not select fallback: %+v %v", result, err)
	}
	destination := MTPDestination{MTPRoute: "sccp-a", PointCode: pc}
	if status, _ := e.MTPDestinationStatus(destination); !status.Congested || status.CongestionLevel != 3 {
		t.Fatalf("inactive reporter hid the active sibling's changed eligibility: %+v", status)
	}
	applyASPDAVA(t, pending, 7, 1, pc, 0)
	if status, _ := e.MTPDestinationStatus(destination); status.Congested {
		t.Fatalf("pending sibling's DAVA did not restore the clear route: %+v", status)
	}
}

func TestTransportCongestionUnusableGatewayCannotHideFallbackPressure(t *testing.T) {
	for _, restricted := range []bool{false, true} {
		name := "available"
		if restricted {
			name = "restricted"
		}
		t.Run(name, func(t *testing.T) {
			cfg := validASPConfig()
			cfg.MaxAffectedPointCodesPerSSNM = 1
			e, associations, _ := newASPTransferFixture(t, cfg)
			for _, association := range associations {
				level := uint8(0)
				if association.cfg.PeerSGP.SignallingGateway == "sg-b" {
					level = 3
				}
				setAssociationTransport(t, e, association, level)
			}
			a, b := associations["sg-a/sgp-a1"], associations["sg-b/sgp-b1"]
			const pc = uint32(0x123456)
			if restricted {
				applyASPDRST(t, b, 9, 42, pc, 0)
			}
			invalidate := func(association *Association, appearance, rc uint32) {
				t.Helper()
				err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(appearance), params.NewRoutingContext(rc), params.NewAffectedPointCode(pc, pc+1), nil))
				if !errors.Is(err, ErrSSNMOversizedReport) {
					t.Fatalf("oversized report: %v", err)
				}
			}
			invalidate(a, 7, 1)
			result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pc, 1, nil)})
			if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].SGP.SignallingGateway != "sg-b" {
				t.Fatalf("unusable gateway was not excluded from transfer: %+v %v", result, err)
			}
			destination := MTPDestination{MTPRoute: "sccp-a", PointCode: pc}
			if status, _ := e.MTPDestinationStatus(destination); !status.Congested || status.CongestionLevel != 3 {
				t.Fatalf("unusable clear gateway hid usable fallback pressure: %+v", status)
			}
			invalidate(b, 9, 42)
			if status, _ := e.MTPDestinationStatus(destination); !status.TransportCongestionUnknown {
				t.Fatalf("no usable gateway was presented as measured clear: %+v", status)
			}
		})
	}
}

func TestTransportCongestionKeepsReceivedAndTransportSourcesOnTheirGateway(t *testing.T) {
	e, associations, _ := newASPTransferFixture(t, validASPConfig())
	for _, association := range associations {
		level := uint8(0)
		if association.cfg.PeerSGP.SignallingGateway == "sg-b" {
			level = 3
		}
		setAssociationTransport(t, e, association, level)
	}
	// SG-A has received congestion but clear transport; SG-B has clear
	// received knowledge but congested transport. Neither route is clear.
	applyASPSCON(t, associations["sg-a/sgp-a1"], 7, 1, 0x123456, 0, params.NewCongestionIndications(3))
	status, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
	if !status.Congested || status.CongestionLevel != 3 {
		t.Fatalf("independent source minima fabricated a clear route: %+v", status)
	}
}

func TestTransportCongestionKeepsReceivedAndTransportSourcesOnTheirAS(t *testing.T) {
	cfg := twoApplicationServerASPConfig()
	sgp := cfg.SignallingGateways[0].SGPs[0]
	sgp.ID = "sgp-a2"
	cfg.SignallingGateways[0].SGPs = append(cfg.SignallingGateways[0].SGPs, sgp)
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	b, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
	setAssociationTransport(t, e, a, 0)
	setAssociationTransport(t, e, b, 3)
	applyASPDAVA(t, a, 7, 1, 0x123456, 0)
	applyASPDAVA(t, b, 7, 2, 0x123456, 0)
	applyASPSCON(t, a, 7, 1, 0x123456, 0, params.NewCongestionIndications(3))
	applyASPSCON(t, b, 7, 2, 0x123456, 0, params.NewCongestionIndications(0))
	status, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
	if !status.Congested || status.CongestionLevel != 3 {
		t.Fatalf("received pressure from one AS cancelled another AS's transport pressure: %+v", status)
	}
}

func TestTransportCongestionSamplingContinuesDuringStateAndProjectionStall(t *testing.T) {
	key := *staticASKey(7, 1)
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "peer", PointCode: 0x123456}, AS: key}}})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	b, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	for _, c := range []*Association{a, b} {
		setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
		c.noteRoutingContextsActive([]uint32{1})
		if !e.trackAssociation(c) {
			t.Fatal("track association")
		}
		setAssociationTransport(t, e, c, 3)
	}
	for len(e.MTPIndications()) > 0 {
		<-e.MTPIndications()
	}
	management := b.ManagementIndications()
	var offset, chargedB atomic.Int64
	chargedB.Store(95)
	m := e.transport
	m.config.Interval = 10 * time.Millisecond
	m.config.AbatementDwell = 30 * time.Millisecond
	m.now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	m.readStatus = nil
	m.read = func(c *Association) (*sctp.SendBacklog, error) {
		charged := 95
		if c == b {
			charged = int(chargedB.Load())
		}
		return &sctp.SendBacklog{Charged: charged, Limit: 100}, nil
	}
	// Only the injected sampler is used; these never touch a socket.
	a.sctpConn, b.sctpConn = &sctp.Conn{}, &sctp.Conn{}
	a.transportReady.Store(true)
	b.transportReady.Store(true)
	locked := false
	t.Cleanup(func() {
		if locked {
			a.muState.Unlock()
		}
		e.cancel(context.Canceled)
		m.workers.Wait()
		a.sctpConn, b.sctpConn = nil, nil
		_ = e.Close()
	})
	e.mu.Lock()
	m.startLocked(e)
	e.mu.Unlock()
	wait := func(condition func() bool, failure string) {
		t.Helper()
		for deadline := time.Now().Add(3 * time.Second); ; {
			if condition() {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal(failure)
			}
			time.Sleep(time.Millisecond)
		}
	}
	wait(func() bool { return a.TransportCongestion().Samples > 0 && b.TransportCongestion().Samples > 0 }, "initial samples did not arrive")
	a.muState.Lock()
	locked = true
	missed := a.TransportCongestion().MissedSamples
	wait(func() bool { return a.TransportCongestion().MissedSamples > missed }, "held state lock did not record a missed slot")
	// Simulate five seconds of lost coverage without a five-second socket or
	// timer wait. Scheduling uses wall time; only observation time advances.
	offset.Store(int64(5 * time.Second))
	chargedB.Store(0)
	before := b.TransportCongestion().Samples
	wait(func() bool {
		first, second := a.TransportCongestion(), b.TransportCongestion()
		return !first.Known && !first.SampleValid && first.Level == 3 && second.Samples >= before+4 && second.Known && second.SampleValid && second.Level == 0
	}, "one state/projection lock stalled other sampling or cleared the held association")
	afterTransition := b.TransportCongestion().Samples
	wait(func() bool {
		state := b.TransportCongestion()
		return state.Samples >= afterTransition+3 && state.SampleValid && state.Known && state.Level == 0
	}, "destination projection occupied a sampling worker after its transition")
	clear := false
	wait(func() bool {
		for len(management) > 0 {
			indication := <-management
			if indication.Kind == ManagementTransportCongestion && indication.TransportCongestion != nil && indication.TransportCongestion.Level == 0 && indication.TransportCongestion.Known {
				clear = true
			}
		}
		return clear
	}, "the unlocked association's Layer Management transition was stalled")
	a.muState.Unlock()
	locked = false
	wait(func() bool {
		for len(e.MTPIndications()) > 0 {
			indication := <-e.MTPIndications()
			if indication.Kind == MTPStatusIndication && !indication.Destination.Congested && !indication.Destination.TransportCongestionUnknown {
				return true
			}
		}
		return false
	}, "coalesced destination recovery was lost after the state lock was released")
}
