package m3ua

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestListenerConfigSelectsAndSnapshotsAssociationConfig(t *testing.T) {
	selected := newSGPAssociationConfigForTest(
		NewHeartbeatInfo(time.Second, 2*time.Second, []byte("beat")),
		1, 2, 3, params.TrafficModeLoadshare, 10, 11,
		[]uint32{7}, params.ServiceIndSCCP, 1, 2, 3,
	)
	selected.Compatibility = AcceptInvalidOptionalInfoString()
	selected.DataQueueSize = 99
	selected.TAck = 250 * time.Millisecond
	selected.TAckRetries = 4
	selected.EstablishTimeout = 3 * time.Second
	selected.TrafficModes = map[uint32]uint32{7: params.TrafficModeOverride}
	selected.SetSCTPSACK(10, 1)
	selected.SetSCTPNoDelay(true)

	listenerConfig := NewListenerConfig(newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		1, 2, 3, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 0,
	))
	listenerConfig.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
		return selected, nil
	}

	snapshot, err := listenerConfig.associationConfigForAccept(AcceptInfo{
		LocalAddr:  mcAddr(2900, "127.0.0.1"),
		RemoteAddr: mcAddr(2901, "127.0.0.2"),
	})
	if err != nil {
		t.Fatalf("connConfigForAccept: %v", err)
	}

	selected.HeartbeatInfo.Interval = 9 * time.Second
	selected.HeartbeatInfo.Data[0] = 'x'
	selected.NetworkAppearance.Data[3] = 99
	selected.RoutingContexts.Data[3] = 99
	selected.TrafficModeType.Data[3] = 99
	selected.TrafficModes[7] = params.TrafficModeBroadcast
	selected.SCTPSACKInfo.SackDelay = 500
	selected.SCTPNoDelayInfo.NoDelay = false
	selected.Compatibility = CompatibilityPolicy{}
	selected.DataQueueSize = 1

	if snapshot == selected {
		t.Fatal("selected AssociationConfig was reused; want an immutable snapshot")
	}
	if snapshot.HeartbeatInfo.Interval != time.Second {
		t.Fatalf("HeartbeatInfo.Interval = %v, want 1s snapshot", snapshot.HeartbeatInfo.Interval)
	}
	if string(snapshot.HeartbeatInfo.Data) != "beat" {
		t.Fatalf("HeartbeatInfo.Data = %q, want copied beat data", snapshot.HeartbeatInfo.Data)
	}
	if got := snapshot.NetworkAppearance.NetworkAppearance(); got != 10 {
		t.Fatalf("NetworkAppearance = %d, want 10", got)
	}
	if got := snapshot.RoutingContexts.RoutingContexts(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("RoutingContexts = %v, want [7]", got)
	}
	if got := snapshot.TrafficModeType.TrafficModeType(); got != params.TrafficModeLoadshare {
		t.Fatalf("TrafficModeType = %d, want Loadshare", got)
	}
	if got := snapshot.TrafficModes[7]; got != params.TrafficModeOverride {
		t.Fatalf("TrafficModes[7] = %d, want Override", got)
	}
	if snapshot.SCTPSACKInfo.SackDelay != 10 {
		t.Fatalf("SackDelay = %d, want 10", snapshot.SCTPSACKInfo.SackDelay)
	}
	if !snapshot.SCTPNoDelayInfo.NoDelay {
		t.Fatal("NoDelay snapshot changed after selected config mutation")
	}
	if snapshot.Compatibility.Tolerator == nil {
		t.Fatal("Compatibility policy was not retained in selected snapshot")
	}
	if snapshot.DataQueueSize != 99 {
		t.Fatalf("DataQueueSize = %d, want 99", snapshot.DataQueueSize)
	}
}

func TestListenerConfigSelectorErrorIsReturned(t *testing.T) {
	want := errors.New("peer rejected")
	listenerConfig := NewListenerConfig(mcSGPConfig())
	listenerConfig.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
		return nil, want
	}

	if _, err := listenerConfig.associationConfigForAccept(AcceptInfo{}); !errors.Is(err, want) {
		t.Fatalf("connConfigForAccept error = %v, want %v", err, want)
	}
}

func TestListenerConfigSelectorOnlyUsesDefaultAssociationConfigFallback(t *testing.T) {
	selected := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		1, 2, 3, params.TrafficModeLoadshare, 10, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 0,
	)
	listener := newSGPListener(&ListenerConfig{
		SelectAssociationConfig: func(AcceptInfo) (*AssociationConfig, error) {
			return selected, nil
		},
	})

	if listener.AssociationConfig == nil {
		t.Fatal("selector-only ListenerConfig left Listener.Config nil")
	}
	if listener.listenerConfig.DefaultAssociationConfig == nil {
		t.Fatal("selector-only ListenerConfig left DefaultAssociationConfig nil")
	}
	snapshot, err := listener.listenerConfig.associationConfigForAccept(AcceptInfo{})
	if err != nil {
		t.Fatalf("connConfigForAccept: %v", err)
	}
	if got := snapshot.NetworkAppearance.NetworkAppearance(); got != 10 {
		t.Fatalf("selected Network Appearance = %d, want 10", got)
	}
}

func TestIPSPListenerSelectorOnlyDefersAssociationConfigValidation(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleIPSP): %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	selected := NewAssociationConfig(1, 2, 3, 0, 0, 1)
	selected.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	listenerConfig := &ListenerConfig{
		SelectAssociationConfig: func(AcceptInfo) (*AssociationConfig, error) {
			return selected, nil
		},
	}
	localAddr, err := sctp.ResolveSCTPAddr("sctp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ResolveSCTPAddr(): %v", err)
	}

	listener, listenErr := endpoint.Listen("m3ua4", localAddr, listenerConfig)
	if listener != nil {
		t.Cleanup(func() { _ = listener.Close() })
	}
	if errors.Is(listenErr, ErrInvalidRoleConfiguration) {
		t.Fatalf("selector-only IPSP Listener validated its unused default config: %v", listenErr)
	}
}

func TestListenerConfigSelectorIsFrozenWhenListenerIsBuilt(t *testing.T) {
	first := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		1, 2, 3, params.TrafficModeLoadshare, 10, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 0,
	)
	second := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		1, 2, 3, params.TrafficModeLoadshare, 20, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 0,
	)
	listenerConfig := NewListenerConfig(first)
	listenerConfig.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
		return first, nil
	}
	listener := newSGPListener(listenerConfig)

	listenerConfig.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
		return second, nil
	}

	selected, err := listener.listenerConfig.associationConfigForAccept(AcceptInfo{})
	if err != nil {
		t.Fatalf("connConfigForAccept: %v", err)
	}
	if got := selected.NetworkAppearance.NetworkAppearance(); got != 10 {
		t.Fatalf("selected Network Appearance = %d, want frozen selector result 10", got)
	}
}

func TestAcceptInfoCarriesOwnedSCTPAddressCopies(t *testing.T) {
	local := mcAddr(2902, "127.0.0.1", "127.0.0.11")
	remote := mcAddr(2903, "127.0.0.2", "127.0.0.22")

	info := newAcceptInfo(local, remote)
	if !sameSCTPAddrPortAndIPs(info.LocalAddr, local) {
		t.Fatalf("local AcceptInfo = %v, want full SCTP address semantics from %v", info.LocalAddr, local)
	}
	if !sameSCTPAddrPortAndIPs(info.RemoteAddr, remote) {
		t.Fatalf("remote AcceptInfo = %v, want full SCTP address semantics from %v", info.RemoteAddr, remote)
	}

	mutateSCTPAddr(info.LocalAddr)
	mutateSCTPAddr(info.RemoteAddr)

	if got := local.IPAddrs[0].IP.String(); got != "127.0.0.1" {
		t.Fatalf("local source address was mutated through AcceptInfo: %s", got)
	}
	if got := remote.IPAddrs[0].IP.String(); got != "127.0.0.2" {
		t.Fatalf("remote source address was mutated through AcceptInfo: %s", got)
	}
}

func TestListenerASKeyAPIsSeparateSameRoutingContextByNetworkAppearance(t *testing.T) {
	listener := newSGPListener(NewListenerConfig(mcSGPConfig()))
	registry, _, _ := listener.registry()

	first, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.as = registry
	first.noteRoutingContextsActive([]uint32{1})
	registry.aspStateChanged(first, StateASPActive)

	second, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.as = registry
	second.noteRoutingContextsActive([]uint32{1})
	registry.aspStateChanged(second, StateASPActive)

	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}

	if got := listener.ActiveASPsForAS(key10); len(got) != 1 || got[0] != first {
		t.Fatalf("ActiveASPsForAS(%v) = %v, want only first ASP", key10, got)
	}
	if got := listener.ActiveASPsForAS(key20); len(got) != 1 || got[0] != second {
		t.Fatalf("ActiveASPsForAS(%v) = %v, want only second ASP", key20, got)
	}
	if got := listener.ActiveASPs(1); got != nil {
		t.Fatalf("legacy ActiveASPs(1) = %v, want nil on ambiguous Network Appearance", got)
	}
}

func mutateSCTPAddr(addr *sctp.SCTPAddr) {
	if addr == nil || len(addr.IPAddrs) == 0 {
		return
	}
	addr.IPAddrs[0].IP = net.IPv4(1, 2, 3, 4)
}

func sameSCTPAddrPortAndIPs(first, second *sctp.SCTPAddr) bool {
	if first == nil || second == nil || first.Port != second.Port || len(first.IPAddrs) != len(second.IPAddrs) {
		return false
	}
	for i := range first.IPAddrs {
		if !first.IPAddrs[i].IP.Equal(second.IPAddrs[i].IP) {
			return false
		}
	}
	return true
}
