package m3ua

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestDisabledTransportCongestionRestartPreservesClearRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var ups, barriers atomic.Int32
	peer := newRawPeer(t, 0, func(message messages.M3UA) messages.M3UA {
		switch m := message.(type) {
		case *messages.HeartbeatAck:
			barriers.Add(1)
			return nil
		case *messages.AspUp:
			ups.Add(1)
			return messages.NewAspUpAck(nil, nil)
		case *messages.AspActive:
			return messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), m.RoutingContext, nil)
		}
		return nil
	})
	cfg := transportBacklogASPConfig()
	cfg.Routing.AllowUnknownDestinations = false
	cfg.Routing.CongestionPolicy = func(_, level uint8, levelSet bool) bool { return levelSet && level < 3 }
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: cfg, TransportCongestion: &TransportCongestionConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	ac := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1})
	ac.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	a, err := e.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.ln.Addr().(*sctp.Addr), ac)
	if err != nil {
		t.Fatal(err)
	}
	destination := MTPDestination{MTPRoute: "sccp-a", PointCode: 0x222222}
	reportClear := func() {
		t.Helper()
		peer.mu.Lock()
		c := peer.conn
		peer.mu.Unlock()
		raw, err := messages.NewDestinationAvailable(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(0x222222), nil).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.SendMsg(raw, sctp.SendOptions{Info: &sctp.SndInfo{PPID: M3UAPPID}}); err != nil {
			t.Fatal(err)
		}
		target := barriers.Load() + 1
		barrier, err := messages.NewHeartbeat(params.NewHeartbeatData([]byte("DAVA committed"))).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.SendMsg(barrier, sctp.SendOptions{Info: &sctp.SndInfo{PPID: M3UAPPID}}); err != nil {
			t.Fatal(err)
		}
		if !waitFor(func() bool { return barriers.Load() >= target }, time.Second) {
			t.Fatal("DAVA dispatch barrier was not acknowledged")
		}
		if !waitFor(func() bool {
			s, ok := e.MTPDestinationStatus(destination)
			return ok && s.Availability == DestinationAvailable
		}, time.Second) {
			t.Fatal("fresh DAVA not applied")
		}
	}
	transfer := func() {
		t.Helper()
		if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x222222, 1, nil)}); err != nil {
			t.Errorf("clear route rejected by congestion policy: %v", err)
		}
	}
	reportClear()
	transfer()
	before, err := a.AssociationStatus()
	if err != nil {
		t.Fatal(err)
	}
	epoch := a.Epoch()
	a.enqueueSCTPRestart()
	if !waitFor(func() bool {
		return ups.Load() >= 2 && a.Epoch() == epoch+1 && a.State() == StateASPActive && a.routingContextAcked(1)
	}, 2*time.Second) {
		t.Fatal("restart did not reactivate the ASP")
	}
	reportClear()
	transfer()
	if s, _ := e.MTPDestinationStatus(destination); s.TransportCongestionUnknown || s.Congested {
		t.Errorf("disabled detector polluted projection after restart: %+v", s)
	}
	after, err := a.AssociationStatus()
	if err != nil {
		t.Fatal(err)
	}
	want := before.TransportCongestion
	want.Epoch = epoch + 1
	if after.TransportCongestion != want {
		t.Errorf("disabled detector status changed: got %+v want %+v", after.TransportCongestion, want)
	}
	if a.transportSnapshot.Load() != nil {
		t.Error("disabled detector acquired a routing contribution")
	}
}

func TestIndependentDimensionAuditOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asp, sgp, err := setupConnHB(t, ctx, 3497, &HeartbeatInfo{Enabled: false}, &HeartbeatInfo{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = asp.Close(); _ = sgp.Close() })
	observeSSNM(t, asp)
	v := destinationRange{NetworkAppearanceSet: true, PointCode: 100, Mask: 1, State: DestinationNetworkState{Availability: DestinationUnavailable}}
	if err := sgp.destinations.setScopedRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.Mask = 0
	v.State.Availability = DestinationAvailable
	if err := sgp.destinations.setScopedRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.Mask = 1
	v.State.Congestion = CongestionState{Congested: true, Level: 2, LevelSet: true}
	if err := sgp.destinations.setScopedCongestionRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if _, err := asp.WriteSignal(messages.NewDestinationStateAudit(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil)); err != nil {
		t.Fatal(err)
	}
	available, congested := false, false
	for !available || !congested {
		report := nextSSNMReport(t, asp)
		switch report.Kind {
		case SSNMDestinationAvailableReport:
			available = true
		case SSNMDestinationUnavailableReport:
			t.Fatal("audit sent stale DUNA instead of the newer narrow DAVA")
		case SSNMSignallingCongestionReport:
			if !report.CongestionLevelSet || report.CongestionLevel != 2 {
				t.Fatalf("audit lost congestion chronology: %+v", report)
			}
			congested = true
		default:
			t.Fatalf("unexpected audit reply: %+v", report)
		}
	}
}
