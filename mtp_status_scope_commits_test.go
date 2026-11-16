package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func newScopedStatusCommitFixture(t *testing.T) (*Endpoint, *Association) {
	t.Helper()
	config := twoApplicationServerASPConfig()
	config.Routing.Paths = []MTPRoutePath{
		{ID: "core", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"as-core"}},
		{ID: "backup", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"as-backup"}},
	}
	config.Routing.MTPRoutes = []MTPRouteConfig{
		{ID: "sccp-a", DestinationPointCode: 0x120000, Mask: 16, Paths: []MTPRoutePathID{"core"}},
		{ID: "sccp-b", DestinationPointCode: 0x120000, Mask: 16, Paths: []MTPRoutePathID{"backup"}},
	}
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config,
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.ssnm.validity = time.Hour // Source retention only, with no automation worker.
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 2)
	for _, rc := range []uint32{1, 2} {
		applyASPDAVA(t, a, 7, rc, 0x120000, 16)
		applyASPSCON(t, a, 7, rc, statusCommitPC, 0, params.NewCongestionIndications(2))
	}
	drainMTPIndications(e.MTPIndications())
	return e, a
}

func scopedStatusOffer(t *testing.T, e *Endpoint, route MTPRouteID) []*MTPIndication {
	t.Helper()
	if _, err := e.MTPTransfer(MTPTransferRequest{MTPRoute: route, ProtocolData: transferProtocolData(statusCommitPC, 0, []byte("offer"))}); err != nil {
		t.Fatal(err)
	}
	indications := takeOfferedIndications(e.MTPIndications())
	for _, indication := range indications {
		if indication.Destination.Destination != (MTPDestination{MTPRoute: route, PointCode: statusCommitPC}) {
			t.Fatalf("offer for %s returned another destination: %+v", route, indication.Destination)
		}
	}
	return indications
}

func TestOfferedStatusScopedOverrideAckCommitsBeforeProjection(t *testing.T) {
	e, a := newScopedStatusCommitFixture(t)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), true)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), true)

	// Override commits before reconciliation reads configured AS keys. Ack
	// itself needs those keys, so move the reconciliation gate to muState
	// before releasing the configuration gate. No membership is written here.
	a.muDynamicASKeys.Lock()
	dynamicLocked, stateLocked := true, false
	defer func() {
		if dynamicLocked {
			a.muDynamicASKeys.Unlock()
		}
		if stateLocked {
			a.muState.Unlock()
		}
	}()
	overridden, acknowledged := make(chan struct{}), make(chan struct{})
	go func() { a.noteRoutingContextsOverridden([]uint32{1}); close(overridden) }()
	waitStatusCommit(t, func() bool {
		a.muAckedRCs.RLock()
		defer a.muAckedRCs.RUnlock()
		_, committed := a.overriddenRCs[1]
		return committed
	})
	a.muState.Lock()
	stateLocked = true
	a.muDynamicASKeys.Unlock()
	dynamicLocked = false
	go func() { a.noteRoutingContextsAcked(params.NewRoutingContext(1)); close(acknowledged) }()
	waitStatusCommit(t, func() bool {
		a.muAckedRCs.RLock()
		defer a.muAckedRCs.RUnlock()
		_, overridden := a.overriddenRCs[1]
		_, acknowledged := a.ackedRCs[1]
		return !overridden && acknowledged
	})
	a.muState.Unlock()
	stateLocked = false
	for _, done := range []<-chan struct{}{overridden, acknowledged} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("scoped lifecycle reconciliation did not complete")
		}
	}
	drainMTPIndications(e.MTPIndications())
	// The same association and DPC serve both routes, but RC1 retirement
	// cannot restart the independent RC2 interval.
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), false)
	requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-a"), offer == 9)
	}
	for offer := 3; offer <= 9; offer++ {
		requireOfferedStatus(t, scopedStatusOffer(t, e, "sccp-b"), offer == 9)
	}
}

func TestOfferedStatusEqualScopedAcknowledgementPreservesBothIntervals(t *testing.T) {
	e, a := newScopedStatusCommitFixture(t)
	for offer := 1; offer <= 9; offer++ {
		a.noteRoutingContextsAcked(params.NewRoutingContext(1))
		for _, route := range []MTPRouteID{"sccp-a", "sccp-b"} {
			requireOfferedStatus(t, scopedStatusOffer(t, e, route), offer == 1 || offer == 9)
		}
	}
}
