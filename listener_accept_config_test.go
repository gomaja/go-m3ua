package m3ua

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestConcurrentAcceptsUseSelectedAssociationConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listenerConfig := NewListenerConfig(mcSGPConfig())
	listenerConfig.SelectAssociationConfig = func(info AcceptInfo) (*AssociationConfig, error) {
		switch {
		case acceptInfoHasRemoteIP(info, "127.0.0.2"):
			return selectedAcceptConfig(10, time.Hour, CompatibilityPolicy{}), nil
		case acceptInfoHasRemoteIP(info, "127.0.0.3"):
			return selectedAcceptConfig(20, 2*time.Hour, AcceptInvalidOptionalInfoString()), nil
		default:
			return nil, errors.New("unexpected peer")
		}
	}

	ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.1"), listenerConfig)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	listenerAddr := ln.Addr()

	type acceptResult struct {
		association *Association
		err         error
	}
	accepted := make(chan acceptResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			association, err := ln.Accept(ctx)
			accepted <- acceptResult{association: association, err: err}
		}()
	}

	for index, ip := range []string{"127.0.0.2", "127.0.0.3"} {
		aspConfig := mcASPConfig(0xCC000001 + uint32(index))
		aspConfig.RoutingContexts = params.NewRoutingContext(1)
		aspConfig.EstablishTimeout = 5 * time.Second
		aspConfig.TAck = 100 * time.Millisecond
		aspConfig.TAckRetries = 5
		aspAssociation, err := dialASP(ctx, "m3ua", mcAddr(0, ip), listenerAddr.(*sctp.SCTPAddr), aspConfig)
		if err != nil {
			t.Fatalf("Dial from %s: %v", ip, err)
		}
		t.Cleanup(func() { _ = aspAssociation.Close() })
	}

	seenNetworkAppearance := make(map[uint32]*Association)
	for i := 0; i < 2; i++ {
		select {
		case result := <-accepted:
			if result.association != nil {
				t.Cleanup(func() { _ = result.association.Close() })
			}
			if result.err != nil {
				t.Fatalf("Accept %d: %v", i, result.err)
			}
			networkAppearance := result.association.cfg.NetworkAppearance.NetworkAppearance()
			seenNetworkAppearance[networkAppearance] = result.association
		case <-time.After(15 * time.Second):
			t.Fatal("Accept did not return for both peers")
		}
	}

	first := seenNetworkAppearance[10]
	second := seenNetworkAppearance[20]
	if first == nil || second == nil {
		t.Fatalf("accepted Network Appearances = %v, want 10 and 20", seenNetworkAppearance)
	}
	if first.hb.Interval != time.Hour {
		t.Fatalf("first heartbeat interval = %v, want 1h", first.hb.Interval)
	}
	if second.hb.Interval != 2*time.Hour {
		t.Fatalf("second heartbeat interval = %v, want 2h", second.hb.Interval)
	}
	if first.cfg.Compatibility.Tolerator != nil {
		t.Fatal("first Association unexpectedly inherited the second peer's compatibility policy")
	}
	if second.cfg.Compatibility.Tolerator == nil {
		t.Fatal("second Association did not receive its selected compatibility policy")
	}

	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	if got := ln.ActiveASPsForAS(key10); len(got) != 1 || got[0] != first {
		t.Fatalf("ActiveASPsForAS(%v) = %v, want first peer only", key10, got)
	}
	if got := ln.ActiveASPsForAS(key20); len(got) != 1 || got[0] != second {
		t.Fatalf("ActiveASPsForAS(%v) = %v, want second peer only", key20, got)
	}
	if got := ln.ActiveASPs(1); got != nil {
		t.Fatalf("legacy ActiveASPs(1) = %v, want nil because RC 1 is ambiguous across Network Appearances", got)
	}
}

func TestAcceptSelectorErrorClosesOnlyRejectedAssociation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	want := errors.New("first peer rejected")
	listenerConfig := NewListenerConfig(mcSGPConfig())
	listenerConfig.SelectAssociationConfig = func(info AcceptInfo) (*AssociationConfig, error) {
		if acceptInfoHasRemoteIP(info, "127.0.0.2") {
			return nil, want
		}
		return selectedAcceptConfig(30, time.Hour, CompatibilityPolicy{}), nil
	}

	ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.1"), listenerConfig)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	listenerAddr := ln.Addr().(*sctp.SCTPAddr)

	type acceptResult struct {
		association *Association
		err         error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		association, err := ln.Accept(ctx)
		accepted <- acceptResult{association: association, err: err}
	}()

	rejectedASPConfig := mcASPConfig(0xDD000001)
	rejectedASPConfig.RoutingContexts = params.NewRoutingContext(1)
	if rejected, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.2"), listenerAddr, rejectedASPConfig); err == nil {
		_ = rejected.Close()
		t.Fatal("Dial succeeded even though the SGP selector rejected the association")
	}
	select {
	case result := <-accepted:
		if result.association != nil {
			_ = result.association.Close()
		}
		if !errors.Is(result.err, want) {
			t.Fatalf("first Accept error = %v, want selector error %v", result.err, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first Accept did not return the selector error")
	}

	accepted = make(chan acceptResult, 1)
	go func() {
		association, err := ln.Accept(ctx)
		accepted <- acceptResult{association: association, err: err}
	}()
	acceptedASPConfig := mcASPConfig(0xDD000002)
	acceptedASPConfig.RoutingContexts = params.NewRoutingContext(1)
	aspAssociation, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.3"), listenerAddr, acceptedASPConfig)
	if err != nil {
		t.Fatalf("second Dial after selector rejection: %v", err)
	}
	t.Cleanup(func() { _ = aspAssociation.Close() })

	select {
	case result := <-accepted:
		if result.association != nil {
			t.Cleanup(func() { _ = result.association.Close() })
		}
		if result.err != nil {
			t.Fatalf("second Accept after selector rejection: %v", result.err)
		}
		if got := result.association.cfg.NetworkAppearance.NetworkAppearance(); got != 30 {
			t.Fatalf("second accepted Network Appearance = %d, want selected config 30", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second Accept did not return after selector rejection")
	}
}

func selectedAcceptConfig(networkAppearance uint32, heartbeatInterval time.Duration, compatibility CompatibilityPolicy) *AssociationConfig {
	config := mcSGPConfig()
	config.RoutingContexts = params.NewRoutingContext(1)
	config.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
	config.HeartbeatInfo = NewHeartbeatInfo(heartbeatInterval, 2*heartbeatInterval, nil)
	config.EstablishTimeout = 5 * time.Second
	config.TAck = 100 * time.Millisecond
	config.TAckRetries = 5
	config.Compatibility = compatibility
	return config
}

func acceptInfoHasRemoteIP(info AcceptInfo, ip string) bool {
	if info.RemoteAddr == nil {
		return false
	}
	parsed := net.ParseIP(ip)
	for _, address := range info.RemoteAddr.IPAddrs {
		if address.IP.Equal(parsed) {
			return true
		}
	}
	return false
}
