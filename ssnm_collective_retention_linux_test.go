package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestIPSPCollectiveCongestionAbatementOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, TransportCongestion: &TransportCongestionConfig{Disabled: true}, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "mapped", PointCode: 100}, AS: *staticASKey(10, 9)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	peer, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, TransportCongestion: &TransportCongestionConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	ac, bc := integrationIPSPConfig(0, 0, true, true), integrationIPSPConfig(0, 0, false, false)
	for _, c := range []*AssociationConfig{ac, bc} {
		setInventoryRoutingContexts(&c.ApplicationServers, params.NewRoutingContext(8, 9))
		setInventoryNetworkAppearance(&c.ApplicationServers, params.NewNetworkAppearance(10))
		c.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	}
	ln, err := peer.Listen("m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(bc))
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan associationResult, 1)
	go func() { a, err := ln.Accept(ctx); accepted <- associationResult{association: a, err: err} }()
	a, err := e.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr), ac)
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
	fillCongestionRecordSlots(t, a, DefaultMaxSSNMDestinationRecords-2)
	send := func(rcs []uint32, level uint8) {
		t.Helper()
		var err error
		if !waitFor(func() bool {
			err = b.SignallingCongestion(SignallingCongestionRequest{Scope: WireScope{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContexts: rcs, RoutingContextSet: true}, Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: level, CongestionLevelSet: true})
			return err == nil || !errors.Is(err, ErrNotEstablished)
		}, time.Second) || err != nil {
			t.Fatalf("send SCON %v level %d: %v", rcs, level, err)
		}
	}
	observed := func(rc uint32, level uint8) bool {
		s, ok := a.destinations.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: rc, routingContextSet: true, pointCode: 100})
		return ok && s.Congestion.LevelSet && s.Congestion.Level == level
	}
	send([]uint32{8, 9}, 2)
	if !waitFor(func() bool {
		s, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "mapped", PointCode: 100})
		return ok && s.Congested && s.CongestionLevelSet && s.CongestionLevel == 2
	}, time.Second) {
		t.Fatal("peer missed initial congestion")
	}
	requireMappedCongestionEvent(t, e, 2)
	send([]uint32{8}, 1)
	if !waitFor(func() bool { return observed(8, 1) }, time.Second) {
		t.Fatal("peer missed newer RC 8 statement")
	}
	send([]uint32{9}, 0)
	if !waitFor(func() bool {
		s, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "mapped", PointCode: 100})
		return ok && !s.Congested && s.CongestionLevelSet && s.CongestionLevel == 0
	}, time.Second) {
		t.Fatal("mapped snapshot missed captured level-zero SCON")
	}
	requireMappedCongestionEvent(t, e, 0)
	t.Log("MTP-STATUS onset=2 abatement=0; mapped snapshot level=0")
}
