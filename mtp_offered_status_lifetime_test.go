package m3ua

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func offerStatusAt(t *testing.T, e *Endpoint, pc uint32) []*MTPIndication {
	t.Helper()
	if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pc, 0, []byte("offer"))}); err != nil {
		t.Fatal(err)
	}
	return takeOfferedIndications(e.MTPIndications())
}
func requireOfferedStatus(t *testing.T, got []*MTPIndication, want bool) {
	t.Helper()
	count := 0
	if want {
		count = 1
	}
	if len(got) != count {
		t.Fatalf("indications=%+v, want count %d", got, count)
	}
	if want {
		i := got[0]
		if i.Kind != MTPStatusIndication || i.Cause != MTPStatusSignallingNetworkCongested || i.Reason != MTPStatusOfferedTraffic || !i.Destination.Congested || i.ResyncRequired {
			t.Fatalf("offered status=%+v", i)
		}
	}
}
func TestInvalidTransferDoesNotAdvanceStatusProcedure(t *testing.T) {
	e, _ := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare)
	invalid := []func(*MTPTransferRequest){
		func(r *MTPTransferRequest) { r.ProtocolData = nil },
		func(r *MTPTransferRequest) { r.ProtocolData.ServiceIndicator = 0 },
		func(r *MTPTransferRequest) { r.ProtocolData.NetworkIndicator = 255 },
		func(r *MTPTransferRequest) { r.ProtocolData.MessagePriority = 1 },
		func(r *MTPTransferRequest) { r.ProtocolData.Data = make([]byte, maxProtocolDataOctets+1) },
		func(r *MTPTransferRequest) { r.ProtocolData.OriginatingPointCode = 0x1000000 },
		func(r *MTPTransferRequest) { r.ProtocolData.DestinationPointCode = 0x1000000 },
		func(r *MTPTransferRequest) { r.MTPRoute = "missing" },
		func(r *MTPTransferRequest) { r.ProtocolData.DestinationPointCode = 0x223456 },
	}
	for _, mutate := range invalid {
		r := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, []byte("invalid"))}
		mutate(&r)
		if _, err := e.MTPTransfer(r); err == nil {
			t.Fatal("invalid request accepted")
		}
		requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), false)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.MTPTransferContext(ctx, MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, nil)}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	// The same invalid requests must not move an already-running interval either.
	for _, mutate := range invalid {
		r := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, nil)}
		mutate(&r)
		_, _ = e.MTPTransfer(r)
	}
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}
func TestOfferedStatusCountsTransportErrorBeforeWrite(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionPrimaryBackup)
	for _, a := range peers {
		a.dataWriter = func([]byte, *sctp.SndInfo) (int, error) { return 0, syscall.EAGAIN }
	}
	_, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, nil)})
	var write *DataWriteError
	if !errors.As(err, &write) || write.Outcome != DataNotSent || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("injected error=%v", err)
	}
	requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), true)
	for _, a := range peers {
		a.dataWriter = func(raw []byte, _ *sctp.SndInfo) (int, error) { return len(raw), nil }
	}
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}
func TestNationalWithPriorityUsesTransitionStatus(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithPriorities, RouteSelectionLoadshare)
	for range 17 {
		r := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, nil)}
		r.ProtocolData.MessagePriority = 2
		if _, err := e.MTPTransfer(r); err != nil {
			t.Fatal(err)
		}
	}
	requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), false)
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, params.NewCongestionIndications(0))
	}
	got := takeOfferedIndications(e.MTPIndications())
	if len(got) != 1 || got[0].Cause != MTPStatusSignallingNetworkCongested || got[0].Reason != MTPStatusTransition || got[0].Destination.Congested {
		t.Fatalf("clear=%+v", got)
	}
}
func TestOfferedStatusDuplicateSCONPreservesInterval(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare)
	for offer := 1; offer <= 17; offer++ {
		for _, a := range peers {
			applyASPSCON(t, a, 7, 1, 0x120000, 16, nil)
		}
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 1 || offer == 9 || offer == 17)
	}
}
func TestExpiryAndClearResetOfferedTrafficCounters(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	now := time.Now()
	e.ssnm.validity = 10 * time.Second
	e.ssnm.now = func() time.Time { return now }
	report := func(level uint8) {
		for _, a := range peers {
			applyASPSCON(t, a, 7, 1, 0x120000, 16, params.NewCongestionIndications(level))
		}
	}
	report(2)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	report(0)
	got := takeOfferedIndications(e.MTPIndications())
	if len(got) != 1 || got[0].Cause != MTPStatusSignallingNetworkCongested || got[0].Reason != MTPStatusTransition || got[0].Destination.Congested {
		t.Fatalf("clear=%+v", got)
	}
	// No offer between clear and recongestion.
	report(2)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	now = now.Add(11 * time.Second)
	e.runCongestionAutomation(now)
	got = takeOfferedIndications(e.MTPIndications())
	if len(got) != 1 || got[0].Cause != MTPStatusCauseNone || got[0].Reason != MTPStatusValidityExpired || !got[0].Destination.ReceivedCongestionUnknown {
		t.Fatalf("expiry=%+v", got)
	}
	for range 17 {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), false)
	}
	report(2)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	// Expiry/recongestion with no offer must also reset.
	now = now.Add(11 * time.Second)
	e.runCongestionAutomation(now)
	report(2)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
}
func TestOfferedStatusUnrelatedExpiryKeepsInterval(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	now := time.Now()
	e.ssnm.validity = 100 * time.Second
	e.ssnm.now = func() time.Time { return now }
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	}
	e.ssnm.validity = time.Second
	// A different exact DPC expires while the original report remains fresh.
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x123499, 0, params.NewCongestionIndications(2))
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	now = now.Add(2 * time.Second)
	e.runCongestionAutomation(now)
	drainMTPIndications(e.MTPIndications())
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}
func TestOfferedStatusExactDPCAndEndpointWideLRU(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, MaxStatusDestinations: 2})
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, nil)
	}
	drainMTPIndications(e.MTPIndications())
	first, second, third := uint32(0x123456), uint32(0x123457), uint32(0x123458)
	requireOfferedStatus(t, offerStatusAt(t, e, first), true)
	requireOfferedStatus(t, offerStatusAt(t, e, second), true)
	requireOfferedStatus(t, offerStatusAt(t, e, first), false) // first is most recently used
	requireOfferedStatus(t, offerStatusAt(t, e, third), true)
	requireOfferedStatus(t, offerStatusAt(t, e, first), false)
	requireOfferedStatus(t, offerStatusAt(t, e, second), true) // evicted => initial again
	if len(e.aspRoutes.statusProcedures) != 2 || e.aspRoutes.statusLRU.Len() != 2 {
		t.Fatal("LRU exceeded Endpoint budget")
	}
	// The limit is a snapshot, not retained application-owned configuration.
}
func TestOfferedStatusBudgetConfiguration(t *testing.T) {
	for _, limit := range []int{-1, 0, 1} {
		config := &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, MaxStatusDestinations: limit}
		e, err := NewEndpoint(EndpointConfig{Role: RoleASP, Congestion: config})
		if limit < 0 {
			if !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("negative limit=%v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		want := limit
		if want == 0 {
			want = 4096
		}
		config.MaxStatusDestinations = 99
		if e.congestion.maxStatusDestinations != want {
			t.Fatalf("limit=%d, want %d", e.congestion.maxStatusDestinations, want)
		}
		_ = e.Close()
	}
}
func TestOfferedStatusRawWritesDoNotCount(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare)
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	var a *Association
	for _, peer := range peers {
		a = peer
		break
	}
	payload := transferProtocolData(0x123456, 0, []byte("raw"))
	for range 8 {
		if _, err := a.WriteData(DataRequest{AS: *staticASKey(7, 1), ProtocolData: *payload}); err != nil {
			t.Fatal(err)
		}
		message := messages.NewData(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewProtocolData(payload.OriginatingPointCode, payload.DestinationPointCode, payload.ServiceIndicator, payload.NetworkIndicator, payload.MessagePriority, payload.SignallingLinkSelection, payload.Data), nil)
		if _, err := a.WriteSignal(message); err != nil {
			t.Fatal(err)
		}
	}
	requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), false)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}
func TestOfferedStatusConcurrentEndpointOffersAndClose(t *testing.T) {
	e, _ := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for sls := range 16 {
		wg.Go(func() {
			for range 16 {
				_, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, uint8(sls), nil)})
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	got := takeOfferedIndications(e.MTPIndications())
	if len(got) != 32 {
		t.Fatalf("256 concurrent offers produced %d statuses, want 32", len(got))
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if len(e.aspRoutes.statusProcedures) != 0 || e.aspRoutes.statusLRU != nil {
		t.Fatal("close retained procedure state")
	}
	if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, nil)}); !errors.Is(err, ErrEndpointClosed) {
		t.Fatal(err)
	}
	// Close may leave a final PAUSE buffered before closing the stream.
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case _, open := <-e.MTPIndications():
			if !open {
				return
			}
		case <-deadline.C:
			t.Fatal("stream remains open after close")
		}
	}
}
func TestOfferedStatusOverflowResyncNeverWaitsForConsumer(t *testing.T) {
	e, _ := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare, 1)
	done := make(chan error, 1)
	go func() {
		for range 25 {
			if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, nil)}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("offering waited for a consumer")
	}
	got := takeOfferedIndications(e.MTPIndications())
	if len(got) != 1 || !got[0].ResyncRequired {
		t.Fatalf("overflow=%+v", got)
	}
	status, known := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
	if !known || !status.Congested {
		t.Fatalf("resync status=%+v known=%v", status, known)
	}
	for offer := 26; offer <= 33; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 33)
	}
}
func TestOfferedStatusLifecycleAndMethodGenerationReset(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare)
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	var a *Association
	for _, peer := range peers {
		a = peer
		break
	}
	e.aspRoutes.detach(a)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	// Retiring a route capability while another peer survives starts a new generation.
	e.aspRoutes.attach(a)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	a.muAckedRCs.Lock()
	delete(a.ackedRCs, 1)
	a.muAckedRCs.Unlock()
	e.aspRoutes.associationStateChanged(a)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	e.aspRoutes.mu.Lock()
	generation := *e.aspRoutes.methods
	e.aspRoutes.methods = &generation
	e.aspRoutes.mu.Unlock()
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
}
func TestOfferedStatusTransitionCauseAndReason(t *testing.T) {
	base := aspDestinationStatus{availability: DestinationAvailable}
	congested := base
	congested.congested = true
	congested.congestionLevel = 2
	congested.congestionLevelSet = true
	expired := base
	expired.receivedUnknown = true
	unavailable := congested
	unavailable.availability = DestinationUnavailable
	for _, tc := range []struct {
		previous, current aspDestinationStatus
		kind              MTPIndicationKind
		cause             MTPStatusCause
		reason            MTPStatusReason
	}{
		{base, congested, MTPStatusIndication, MTPStatusSignallingNetworkCongested, MTPStatusTransition},
		{congested, base, MTPStatusIndication, MTPStatusSignallingNetworkCongested, MTPStatusTransition},
		{congested, expired, MTPStatusIndication, MTPStatusCauseNone, MTPStatusTransition},
		{congested, unavailable, MTPPauseIndication, MTPStatusCauseNone, MTPStatusTransition},
		{unavailable, congested, MTPResumeIndication, MTPStatusCauseNone, MTPStatusTransition},
	} {
		i := newMTPIndication(aspDerivedRangeKey{}, tc.previous, tc.current)
		if i == nil || i.Kind != tc.kind || i.Cause != tc.cause || i.Reason != tc.reason {
			t.Fatalf("transition=%+v want=%+v", i, tc)
		}
	}
	if !reflect.DeepEqual(MTPIndication{}, MTPIndication{Cause: MTPStatusCauseNone, Reason: MTPStatusTransition}) {
		t.Fatal("zero values lost")
	}
}

func TestOfferedStatusRepeatedPolicyEvaluationCountsOnce(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	calls := 0
	config.Routing.CongestionPolicy = func(uint8, uint8, bool) bool { calls++; return true }
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, nil)
	}
	drainMTPIndications(e.MTPIndications())
	for offer := 1; offer <= 17; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer%8 == 1)
	}
	if calls <= 17 {
		t.Fatalf("policy evaluated only %d times; repeated-evaluation seam not exercised", calls)
	}
}
func TestOfferedStatusRoutesShareEndpointBudgetButNotCounters(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	second := config.Routing.MTPRoutes[0]
	second.ID = "sccp-b"
	config.Routing.MTPRoutes = append(config.Routing.MTPRoutes, second)
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, MaxStatusDestinations: 1})
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, nil)
	}
	drainMTPIndications(e.MTPIndications())
	for _, route := range []MTPRouteID{"sccp-a", "sccp-b", "sccp-a"} {
		_, err := e.MTPTransfer(MTPTransferRequest{MTPRoute: route, ProtocolData: transferProtocolData(0x123456, 0, nil)})
		if err != nil {
			t.Fatal(err)
		}
		got := takeOfferedIndications(e.MTPIndications())
		requireOfferedStatus(t, got, true)
		if got[0].Destination.Destination.MTPRoute != route || len(e.aspRoutes.statusProcedures) != 1 {
			t.Fatalf("route counter or global budget leaked: %+v", got)
		}
	}
}

func TestOfferedStatusStaggeredExpiryNeverReportsMeasuredClear(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	now := time.Now()
	e.ssnm.validity = 10 * time.Second
	e.ssnm.now = func() time.Time { return now }
	applyASPSCON(t, peers["sg-a/sgp-a1"], 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	now = now.Add(5 * time.Second)
	applyASPSCON(t, peers["sg-a/sgp-a2"], 7, 1, 0x120000, 16, params.NewCongestionIndications(3))
	drainMTPIndications(e.MTPIndications())
	for _, advance := range []time.Duration{6 * time.Second, 5 * time.Second} {
		now = now.Add(advance)
		e.runCongestionAutomation(now)
		got := takeOfferedIndications(e.MTPIndications())
		if len(got) != 1 {
			t.Fatalf("staggered expiry indication count=%d", len(got))
		}
		if got[0].Cause != MTPStatusCauseNone || got[0].Reason != MTPStatusValidityExpired || !got[0].Destination.ReceivedCongestionUnknown {
			t.Fatalf("staggered expiry=%+v", *got[0])
		}
	}
	// A fresh source can start measured congestion while an expired sibling
	// remains unknown. This is an onset, not a second expiry event.
	applyASPSCON(t, peers["sg-a/sgp-a1"], 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	got := takeOfferedIndications(e.MTPIndications())
	if len(got) != 1 || got[0].Cause != MTPStatusSignallingNetworkCongested || got[0].Reason != MTPStatusTransition || !got[0].Destination.Congested {
		t.Fatalf("fresh onset=%+v", got)
	}
}

func TestOfferedStatusTransportUncertaintyRetiresIntervalWithoutTraffic(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	pressure := func(known bool) {
		for _, a := range peers {
			a.transportSnapshot.Store(&TransportCongestionState{Known: known, Epoch: a.Epoch()})
			e.aspRoutes.transportChanged(a)
		}
	}
	pressure(false)
	drainMTPIndications(e.MTPIndications())
	pressure(true)
	drainMTPIndications(e.MTPIndications())
	// Unknown and recovery occurred entirely between offers.
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	pressure(false)
	drainMTPIndications(e.MTPIndications())
	for range 17 {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), false)
	}
	pressure(true)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
}

func TestOfferedStatusIPSPMappingUsesExplicitProcedure(t *testing.T) {
	destination := MTPDestination{MTPRoute: "peer-sccp", PointCode: 100}
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}, IPSPDestinations: []IPSPDestinationConfig{{Destination: destination, AS: *staticASKey(7, 1)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	a.noteRoutingContextsActive([]uint32{1})
	a.noteRoutingContextsAcked(params.NewRoutingContext(1))
	if !e.trackAssociation(a) {
		t.Fatal("track IPSP")
	}
	a.dataWriter = func(raw []byte, _ *sctp.SndInfo) (int, error) { return len(raw), nil }
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatal(err)
	}
	drainMTPIndications(e.MTPIndications())
	status, known := e.MTPDestinationStatus(destination)
	if !known || !status.Congested {
		t.Fatalf("mapped status=%+v known=%v", status, known)
	}
	p, err := NewMTPStatusProcedure(CongestionProfile{Method: CongestionNationalWithoutPriorities})
	if err != nil {
		t.Fatal(err)
	}
	var due []int
	for offer := 1; offer <= 17; offer++ {
		if indication := offeredProcedureStatus(t, p, status); indication != nil {
			if indication.Destination != status {
				t.Fatalf("mapped primitive=%+v", indication)
			}
			due = append(due, offer)
		}
		if _, err := a.WriteData(DataRequest{AS: *staticASKey(7, 1), ProtocolData: *transferProtocolData(100, 0, nil)}); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(due, []int{1, 9, 17}) {
		t.Fatalf("explicit IPSP status offers=%v", due)
	}
	requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), false)
}

func TestOfferedStatusUnrelatedPeerTimerKeepsInterval(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	// This provisioned peer owns no path of the MTP route being offered.
	config.SignallingGateways = append(config.SignallingGateways, validASPConfig().SignallingGateways[1])
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	for _, key := range []string{"sg-a/sgp-a1", "sg-a/sgp-a2"} {
		applyASPSCON(t, peers[key], 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	now := time.Now()
	e.ssnm.validity = time.Second
	e.ssnm.now = func() time.Time { return now }
	applyASPSCON(t, peers["sg-b/sgp-b1"], 9, 42, 0x123456, 0, params.NewCongestionIndications(3))
	now = now.Add(2 * time.Second)
	e.runCongestionAutomation(now)
	requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), false)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), offer == 9)
	}
}

func TestOfferedStatusPolicyVetoStillCountsOriginalOffer(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	config.Routing.CongestionPolicy = func(uint8, uint8, bool) bool { return false }
	e, peers, captures := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, nil)
	}
	drainMTPIndications(e.MTPIndications())
	for offer := 1; offer <= 17; offer++ {
		_, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, nil)})
		if !errors.Is(err, ErrNoMTPRoute) {
			t.Fatalf("vetoed offer=%v", err)
		}
		requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), offer%8 == 1)
	}
	for _, capture := range captures {
		if capture.count() != 0 {
			t.Fatal("veto submitted DATA")
		}
	}
}

func TestOfferedStatusExpiryWithFreshAlternativeKeepsExpiryReason(t *testing.T) {
	e, peers, _ := newASPTransferFixtureReporting(t, validASPConfig(), true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	now := time.Now()
	e.ssnm.validity = 10 * time.Second
	e.ssnm.now = func() time.Time { return now }
	applyASPSCON(t, peers["sg-a/sgp-a1"], 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	now = now.Add(5 * time.Second)
	applyASPSCON(t, peers["sg-b/sgp-b1"], 9, 42, 0x120000, 16, params.NewCongestionIndications(3))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
	now = now.Add(6 * time.Second)
	e.runCongestionAutomation(now)
	got := takeOfferedIndications(e.MTPIndications())
	if len(got) != 1 {
		t.Fatalf("expiry indication count=%d", len(got))
	}
	indication := got[0]
	if indication.Destination.ReceivedCongestionUnknown || indication.Destination.TransportCongestionUnknown {
		t.Fatalf("fixture must retain a known fresh alternative: %+v", *indication)
	}
	if indication.Cause != MTPStatusCauseNone || indication.Reason != MTPStatusValidityExpired || !indication.Destination.Congested || indication.Destination.CongestionLevel != 3 {
		t.Fatalf("expiry with surviving alternative=%+v", *indication)
	}
	status, known := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
	if !known || status.ReceivedCongestionUnknown || status.TransportCongestionUnknown {
		t.Fatalf("fresh alternative is not known: %+v known=%v", status, known)
	}
	// The fresh known alternative starts a new interval after effective expiry.
	requireOfferedStatus(t, offerStatusAt(t, e, 0x123456), true)
}
