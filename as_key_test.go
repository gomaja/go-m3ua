package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestApplicationServersKeyIncludesNetworkAppearance(t *testing.T) {
	registry := newApplicationServers(DefaultRecoveryTimer, nil)

	first, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.as = registry
	first.noteRoutingContextsActive([]uint32{1})
	registry.aspStateChanged(first, StateAspActive)

	second, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.as = registry
	second.noteRoutingContextsActive([]uint32{1})
	registry.aspStateChanged(second, StateAspActive)

	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}

	if got := registry.get(key10).activeASPs(); len(got) != 1 || got[0] != first {
		t.Fatalf("AS %v active ASPs = %v, want only first ASP", key10, got)
	}
	if got := registry.get(key20).activeASPs(); len(got) != 1 || got[0] != second {
		t.Fatalf("AS %v active ASPs = %v, want only second ASP", key20, got)
	}
}

func TestApplicationServersSupportContextlessASKeysPerNetworkAppearance(t *testing.T) {
	registry := newApplicationServers(DefaultRecoveryTimer, nil)

	first, _ := newTestConnWithContexts(t, StateAspActive, modeServer)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.as = registry
	first.noteRoutingContextsActive(nil)
	registry.aspStateChanged(first, StateAspActive)

	second, _ := newTestConnWithContexts(t, StateAspActive, modeServer)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.as = registry
	second.noteRoutingContextsActive(nil)
	registry.aspStateChanged(second, StateAspActive)

	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true}
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true}

	if got := registry.get(key10).activeASPs(); len(got) != 1 || got[0] != first {
		t.Fatalf("contextless AS %v active ASPs = %v, want only first ASP", key10, got)
	}
	if got := registry.get(key20).activeASPs(); len(got) != 1 || got[0] != second {
		t.Fatalf("contextless AS %v active ASPs = %v, want only second ASP", key20, got)
	}
}

func TestContextlessASRejectsIncompatibleTrafficModeForSameASKey(t *testing.T) {
	registry := newApplicationServers(DefaultRecoveryTimer, nil)

	first, _ := newTestConnWithContexts(t, StateAspInactive, modeServer)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	first.as = registry
	if err := first.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("first contextless ASP Active: %v", err)
	}

	second, _ := newTestConnWithContexts(t, StateAspInactive, modeServer)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	second.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
	second.as = registry
	err := second.handleAspActive(messages.NewAspActive(nil, nil, nil))
	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("second contextless ASP Active error = %v, want ErrUnsupportedTrafficMode", err)
	}
}
