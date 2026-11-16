package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestOfferedStatusTransportClearSurvivesCoalescedProjection(t *testing.T) {
	exerciseCoalescedMTPTransportEnd(t, false)
}

func TestOfferedStatusTransportStalenessSurvivesCoalescedProjection(t *testing.T) {
	exerciseCoalescedMTPTransportEnd(t, true)
}

func TestOfferedStatusTransportOnsetAfterOfferDuringUnprojectedClear(t *testing.T) {
	exerciseCoalescedMTPTransportEnd(t, false, true)
}

func TestOfferedStatusTransportOnsetAfterOfferDuringUnprojectedStaleness(t *testing.T) {
	exerciseCoalescedMTPTransportEnd(t, true, true)
}

func exerciseCoalescedMTPTransportEnd(t *testing.T, stale bool, intermediateOffer ...bool) {
	t.Helper()
	for _, projectFirst := range []bool{false, true} {
		name := "offer-before-projection"
		if projectFirst {
			name = "projection-before-offer"
		}
		t.Run(name, func(t *testing.T) {
			config := validASPConfig()
			useSignallingGateways(config, "sg-a")
			e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
			a := peers["sg-a/sgp-a1"]
			m := e.transport
			now := time.Unix(1000, 0)
			charged := 95
			m.now = func() time.Time { return now }
			m.readStatus = nil
			m.read = func(*Association) (*sctp.SendBacklog, error) {
				return &sctp.SendBacklog{Charged: charged, Limit: 100}, nil
			}
			m.config.OnsetDwell = time.Nanosecond
			m.config.AbatementDwell = time.Nanosecond
			job := transportCongestionJob{a, a.ID(), a.Epoch()}
			sample := func() { now = now.Add(100 * time.Millisecond); m.sample(e, job) }
			sample()
			sample()
			if state := a.TransportCongestion(); !state.Known || state.Level != 3 {
				t.Fatalf("onset control=%+v", state)
			}
			drainMTPIndications(e.MTPIndications())
			management := a.ManagementIndications()
			for len(management) > 0 {
				<-management
			}
			requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
			sample() // Equal positive observations preserve the interval.
			sample()
			requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), false)
			// Use the production pending-bit path while deliberately withholding its worker.
			e.mu.Lock()
			if m.started {
				e.mu.Unlock()
				t.Fatal("fixture unexpectedly started sampler")
			}
			m.started = true
			m.projection = make(chan struct{}, 1)
			e.mu.Unlock()
			if stale {
				now = now.Add(m.config.StaleAfter)
				m.missed(e, job)
			} else {
				charged = 0
				sample()
				sample()
			}
			if len(intermediateOffer) > 0 && intermediateOffer[0] {
				state := a.TransportCongestion()
				if stale && state.Known || !stale && (!state.Known || state.Level != 0) {
					t.Fatalf("transport ending did not commit: %+v", state)
				}
				if !a.transportProjectionPending.Load() {
					t.Fatal("ending projection was not deferred")
				}
				// Consume the ending reset against the old projected positive
				// status. The subsequent real onset needs its own reset.
				requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
			}
			charged = 95
			sample()
			sample()
			if !a.transportProjectionPending.Load() {
				t.Fatal("projection was not deferred")
			}
			var levels []uint8
			for len(management) > 0 {
				i := <-management
				if i.Kind == ManagementTransportCongestion && i.TransportCongestion != nil {
					levels = append(levels, i.TransportCongestion.Level)
				}
			}
			firstLevel := uint8(0)
			if stale {
				firstLevel = 3
			}
			if len(levels) != 2 || levels[0] != firstLevel || levels[1] != 3 {
				t.Fatalf("management clear/onset=%v", levels)
			}
			if state := a.TransportCongestion(); !state.Known || state.Level != 3 {
				t.Fatalf("recongestion control=%+v", state)
			}
			// The latest projection equals the old congested state, but the episode ended.
			if projectFirst {
				m.project(e, a)
				drainMTPIndications(e.MTPIndications())
			}
			requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
			for offer := 2; offer <= 9; offer++ {
				requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
			}
		})
	}
}

func TestOfferedStatusTransportRestartsPreserveUnrelatedRoute(t *testing.T) {
	config := validASPConfig()
	config.Routing.MTPRoutes[0].Paths = []MTPRoutePathID{"sg-a-core"}
	other := config.Routing.MTPRoutes[0]
	other.ID = "sccp-b"
	other.Paths = []MTPRoutePathID{"sg-b-core"}
	config.Routing.MTPRoutes = append(config.Routing.MTPRoutes, other)
	e, peers, _ := newASPTransferFixtureReporting(t, config, true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	a, b := peers["sg-a/sgp-a1"], peers["sg-b/sgp-b1"]
	m := e.transport
	now := time.Unix(1000, 0)
	chargedA := 95
	m.now = func() time.Time { return now }
	m.readStatus = nil
	m.read = func(peer *Association) (*sctp.SendBacklog, error) {
		charged := 95
		if peer == a {
			charged = chargedA
		}
		return &sctp.SendBacklog{Charged: charged, Limit: 100}, nil
	}
	m.config.OnsetDwell = time.Nanosecond
	m.config.AbatementDwell = time.Nanosecond
	sample := func(peer *Association) {
		now = now.Add(100 * time.Millisecond)
		m.sample(e, transportCongestionJob{peer, peer.ID(), peer.Epoch()})
	}
	for _, peer := range []*Association{a, b} {
		sample(peer)
		sample(peer)
		if state := peer.TransportCongestion(); !state.Known || state.Level != 3 {
			t.Fatalf("initial transport congestion=%+v", state)
		}
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), true)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), true)
	e.mu.Lock()
	if m.started {
		e.mu.Unlock()
		t.Fatal("fixture unexpectedly started sampler")
	}
	m.started = true
	m.projection = make(chan struct{}, 1)
	e.mu.Unlock()
	chargedA = 0
	sample(a)
	sample(a)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), true)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), false)
	chargedA = 95
	sample(a)
	sample(a)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), true)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), false)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), offer == 9)
	}
	for offer := 4; offer <= 9; offer++ {
		requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), offer == 9)
	}
	// A changed positive transport level also carries a new status, while the
	// other route's continuing level keeps its own cadence.
	for _, charged := range []int{75, 95} {
		chargedA = charged
		sample(a)
		sample(a)
		wantLevel := uint8(2)
		if charged == 95 {
			wantLevel = 3
		}
		if state := a.TransportCongestion(); !state.Known || state.Level != wantLevel {
			t.Fatalf("changed positive transport level=%+v, want %d", state, wantLevel)
		}
		requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), true)
		requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), false)
	}
}

func TestOfferedStatusTransportSnapshotCommitWindow(t *testing.T) {
	config := validASPConfig()
	useSignallingGateways(config, "sg-a")
	e, peers, _ := newASPTransferFixtureReporting(t, config, true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	a := peers["sg-a/sgp-a1"]
	a.transportSnapshot.Store(&TransportCongestionState{Epoch: a.Epoch(), Known: true, Level: 3})
	e.transport.project(e, a)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)

	a.transportMu.Lock()
	clear := TransportCongestionState{Epoch: a.Epoch(), Known: true}
	finishClear := a.beginTransportStatusChange(a.TransportCongestion(), clear, false)
	a.transportSnapshot.Store(&clear)
	finishClear()
	a.transportMu.Unlock()
	// Keep the old positive projection, but consume the committed ending.
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)

	a.transportMu.Lock()
	defer a.transportMu.Unlock()
	onset := TransportCongestionState{Epoch: a.Epoch(), Known: true, Level: 3}
	finishOnset := a.beginTransportStatusChange(clear, onset, false)
	defer finishOnset()
	_ = offerStatusAt(t, e, 0x123456) // May consume a pre-publication hint.
	a.transportSnapshot.Store(&onset)
	// This offer falls after snapshot publication and before completion has
	// posted its final pending bit. It still starts the new interval.
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
}

func TestOfferedStatusTransportOnsetBeforeActivationProjection(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: validASPConfig(),
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	a.transportSnapshot.Store(&TransportCongestionState{Epoch: a.Epoch(), Known: true, Level: 2})
	applyASPDAVA(t, a, 7, 1, statusCommitPC, 0)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	b := attachActivatingSSNMAssociation(t, e, SGPIdentity{SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b1"}, 9, 42)
	applyASPDAVA(t, b, 9, 42, statusCommitPC, 0)
	drainMTPIndications(e.MTPIndications())
	_ = offerStatusAt(t, e, statusCommitPC)

	m := e.transport
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	m.readStatus = nil
	m.read = func(*Association) (*sctp.SendBacklog, error) {
		return &sctp.SendBacklog{Charged: 95, Limit: 100}, nil
	}
	m.config.OnsetDwell = time.Nanosecond
	e.mu.Lock()
	m.started = true
	m.projection = make(chan struct{}, 1)
	e.mu.Unlock()
	r := e.aspRoutes
	r.mu.Lock()
	locked := true
	defer func() {
		if locked {
			r.mu.Unlock()
		}
	}()
	route := r.config.mtpRoutes[r.config.mtpRouteByID["sccp-a"]]
	assertCongestion := func(want bool) {
		t.Helper()
		status, known := offeredDestinationStatus(route, statusCommitPC, r.recomputeMTPRouteLocked(route))
		if !known || status.congested != want || status.transportUnknown {
			t.Fatalf("activation transport status=%+v known=%v, want congestion=%v", status, known, want)
		}
	}
	assertCongestion(true)
	activated := make(chan bool, 1)
	go func() { activated <- b.commitState(StateASPActive) }()
	waitStatusCommit(t, func() bool {
		e.ssnm.mu.Lock()
		defer e.ssnm.mu.Unlock()
		state := e.ssnm.partitions[canonicalSSNMPartition("sg-b", "as-core")]
		return state != nil && state.trafficAuthorized()
	})
	assertCongestion(false)
	// Consume the activation mark before this new member's transport onset.
	r.offerMTPTransferLocked(route, MTPTransferRequest{ProtocolData: transferProtocolData(statusCommitPC, 0, []byte("offer"))})
	_ = takeOfferedIndications(e.MTPIndications())
	for range 2 {
		now = now.Add(100 * time.Millisecond)
		m.sample(e, transportCongestionJob{b, b.ID(), b.Epoch()})
	}
	if state := b.TransportCongestion(); !state.Known || state.Level != 3 {
		t.Fatalf("new member transport onset=%+v", state)
	}
	assertCongestion(true)
	r.offerMTPTransferLocked(route, MTPTransferRequest{ProtocolData: transferProtocolData(statusCommitPC, 0, []byte("offer"))})
	requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), true)
	r.mu.Unlock()
	locked = false
	select {
	case accepted := <-activated:
		if !accepted {
			t.Fatal("activation rejected")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("activation projection did not finish")
	}
}

func TestOfferedStatusEarlierExpiryIsNotNewNotificationTrigger(t *testing.T) {
	for _, trigger := range []string{"availability", "transport", "lifecycle"} {
		t.Run(trigger, func(t *testing.T) {
			e, peers, _ := newASPTransferFixtureReporting(t, validASPConfig(), true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
			now := time.Now()
			e.ssnm.validity = 10 * time.Second
			e.ssnm.now = func() time.Time { return now }
			a, b := peers["sg-a/sgp-a1"], peers["sg-b/sgp-b1"]
			applyASPSCON(t, a, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
			now = now.Add(5 * time.Second)
			applyASPSCON(t, b, 9, 42, 0x120000, 16, params.NewCongestionIndications(3))
			now = now.Add(6 * time.Second)
			e.runCongestionAutomation(now)
			status, known := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
			if !known || !status.Congested || status.ReceivedCongestionUnknown {
				t.Fatalf("fresh-alternative control=%+v known=%v", status, known)
			}
			drainMTPIndications(e.MTPIndications())
			switch trigger {
			case "availability":
				applyASPDUNA(t, b, 9, 42, 0x120000, 16)
			case "transport":
				b.transportSnapshot.Store(&TransportCongestionState{Epoch: b.Epoch(), Known: false})
				e.aspRoutes.transportChanged(b)
			case "lifecycle":
				e.aspRoutes.detach(b)
			}
			got := takeOfferedIndications(e.MTPIndications())
			if len(got) != 1 {
				t.Fatalf("notification count=%d", len(got))
			}
			i := got[0]
			if !i.Destination.ReceivedCongestionUnknown {
				t.Fatalf("earlier expired source not exposed: %+v", *i)
			}
			if i.Reason != MTPStatusTransition || i.Cause != MTPStatusCauseNone {
				t.Fatalf("%s exposed old expiry but notification=%+v", trigger, *i)
			}
			requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), false)
		})
	}
}

func TestOfferedStatusTransportInactiveSampleCrossesActivation(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: validASPConfig(),
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	a.transportSnapshot.Store(&TransportCongestionState{Epoch: a.Epoch(), Known: true, Level: 2})
	applyASPDAVA(t, a, 7, 1, statusCommitPC, 0)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	b := attachActivatingSSNMAssociation(t, e, SGPIdentity{SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b1"}, 9, 42)
	applyASPDAVA(t, b, 9, 42, statusCommitPC, 0)
	drainMTPIndications(e.MTPIndications())
	_ = offerStatusAt(t, e, statusCommitPC)

	m := e.transport
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	m.readStatus = nil
	m.read = func(*Association) (*sctp.SendBacklog, error) {
		return &sctp.SendBacklog{Charged: 95, Limit: 100}, nil
	}
	m.config.OnsetDwell = time.Nanosecond
	e.mu.Lock()
	m.started = true
	m.projection = make(chan struct{}, 1)
	e.mu.Unlock()
	r := e.aspRoutes
	r.mu.Lock()
	locked := true
	defer func() {
		if locked {
			r.mu.Unlock()
		}
	}()
	route := r.config.mtpRoutes[r.config.mtpRouteByID["sccp-a"]]
	assertCongestion := func(want bool) {
		t.Helper()
		status, known := offeredDestinationStatus(route, statusCommitPC, r.recomputeMTPRouteLocked(route))
		if !known || status.congested != want || status.transportUnknown {
			t.Fatalf("activation transport status=%+v known=%v, want congestion=%v", status, known, want)
		}
	}
	assertCongestion(true)
	e.mu.Lock()
	defer e.mu.Unlock()
	b.transportMu.Lock()
	defer b.transportMu.Unlock()
	onset := TransportCongestionState{Epoch: b.Epoch(), Known: true, Level: 3}
	finishOnset := b.beginTransportStatusChange(b.TransportCongestion(), onset, true)
	defer finishOnset()
	activated := make(chan bool, 1)
	go func() { activated <- b.commitState(StateASPActive) }()
	waitStatusCommit(t, func() bool {
		e.ssnm.mu.Lock()
		defer e.ssnm.mu.Unlock()
		state := e.ssnm.partitions[canonicalSSNMPartition("sg-b", "as-core")]
		return state != nil && state.trafficAuthorized()
	})
	assertCongestion(false)
	// Consume the activation mark before this new member's transport onset.
	r.offerMTPTransferLocked(route, MTPTransferRequest{ProtocolData: transferProtocolData(statusCommitPC, 0, []byte("offer"))})
	_ = takeOfferedIndications(e.MTPIndications())
	b.transportSnapshot.Store(&onset)
	if state := b.TransportCongestion(); !state.Known || state.Level != 3 {
		t.Fatalf("new member transport onset=%+v", state)
	}
	assertCongestion(true)
	r.offerMTPTransferLocked(route, MTPTransferRequest{ProtocolData: transferProtocolData(statusCommitPC, 0, []byte("offer"))})
	requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), true)
	r.mu.Unlock()
	locked = false
	select {
	case accepted := <-activated:
		if !accepted {
			t.Fatal("activation rejected")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("activation projection did not finish")
	}
}

func TestOfferedStatusTransportInactiveSamplesPreserveCadence(t *testing.T) {
	e, peers, _ := newASPTransferFixtureReporting(t, validASPConfig(), true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	a, b := peers["sg-a/sgp-a1"], peers["sg-b/sgp-b1"]
	if !b.commitState(StateASPInactive) {
		t.Fatal("inactive transition rejected")
	}
	a.transportSnapshot.Store(&TransportCongestionState{Epoch: a.Epoch(), Known: true, Level: 2})
	e.transport.project(e, a)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	m := e.transport
	now := time.Unix(1000, 0)
	charged := 95
	m.now = func() time.Time { return now }
	m.readStatus = nil
	m.read = func(*Association) (*sctp.SendBacklog, error) {
		return &sctp.SendBacklog{Charged: charged, Limit: 100}, nil
	}
	m.config.OnsetDwell = time.Nanosecond
	m.config.AbatementDwell = time.Nanosecond
	for _, pressure := range []int{95, 0} {
		charged = pressure
		for range 2 {
			now = now.Add(100 * time.Millisecond)
			m.sample(e, transportCongestionJob{b, b.ID(), b.Epoch()})
		}
		wantLevel := uint8(0)
		if pressure > 0 {
			wantLevel = 3
		}
		if state := b.TransportCongestion(); !state.Known || state.Level != wantLevel {
			t.Fatalf("inactive sample=%+v, want level %d", state, wantLevel)
		}
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), false)
	}
	for offer := 4; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}

func TestOfferedStatusTransportInactiveEpochRetiresInterval(t *testing.T) {
	e, peers, _ := newASPTransferFixtureReporting(t, validASPConfig(), true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	a, b := peers["sg-a/sgp-a1"], peers["sg-b/sgp-b1"]
	if !b.commitState(StateASPInactive) {
		t.Fatal("inactive transition rejected")
	}
	a.transportSnapshot.Store(&TransportCongestionState{Epoch: a.Epoch(), Known: true, Level: 2})
	e.transport.project(e, a)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	b.resetTransportCongestionEpoch()
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}
