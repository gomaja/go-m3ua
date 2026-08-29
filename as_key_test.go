package m3ua

import (
	"errors"
	"strconv"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestApplicationServersKeyIncludesNetworkAppearance(t *testing.T) {
	registry := newApplicationServers(DefaultRecoveryTimer, nil)

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

	if got := registry.get(key10).activeASPs(); len(got) != 1 || got[0] != first {
		t.Fatalf("AS %v active ASPs = %v, want only first ASP", key10, got)
	}
	if got := registry.get(key20).activeASPs(); len(got) != 1 || got[0] != second {
		t.Fatalf("AS %v active ASPs = %v, want only second ASP", key20, got)
	}
}

func TestApplicationServersSupportContextlessASKeysPerNetworkAppearance(t *testing.T) {
	registry := newApplicationServers(DefaultRecoveryTimer, nil)

	first, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.as = registry
	first.noteRoutingContextsActive(nil)
	registry.aspStateChanged(first, StateASPActive)

	second, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.as = registry
	second.noteRoutingContextsActive(nil)
	registry.aspStateChanged(second, StateASPActive)

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

	first, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	first.as = registry
	if err := first.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("first contextless ASP Active: %v", err)
	}

	second, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	second.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
	second.as = registry
	err := second.handleAspActive(messages.NewAspActive(nil, nil, nil))
	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("second contextless ASP Active error = %v, want ErrUnsupportedTrafficMode", err)
	}
}

func TestContextlessASAllowsDifferentNetworkAppearanceTrafficModes(t *testing.T) {
	registry := newApplicationServers(DefaultRecoveryTimer, nil)

	first, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	first.as = registry
	if err := first.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("first contextless ASP Active: %v", err)
	}

	second, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
	second.as = registry
	if err := second.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("second contextless ASP Active in another Network Appearance: %v", err)
	}
}

func TestNormalizeASKeyRejectsOutOfRangeIntRoutingContext(t *testing.T) {
	if _, ok := normalizeASKey(-1); ok {
		t.Fatal("package AS key normalization accepted a negative int Routing Context")
	}
	if _, ok := legacyRoutingContextScope(-1); ok {
		t.Fatal("legacy Routing Context scope accepted a negative int")
	}

	if strconv.IntSize <= 32 {
		return
	}
	aboveMax := uint64(maxRoutingContextValue)
	aboveMax++
	outOfRange := int(aboveMax)
	if _, ok := normalizeASKey(outOfRange); ok {
		t.Fatal("package AS key normalization accepted an int above uint32")
	}
	if _, ok := legacyRoutingContextScope(outOfRange); ok {
		t.Fatal("legacy Routing Context scope accepted an int above uint32")
	}
	registry := newApplicationServers(DefaultRecoveryTimer, nil)
	if _, ok := registry.normalizeASKey(outOfRange); ok {
		t.Fatal("registry AS key normalization accepted an int above uint32")
	}
}

func TestContextlessOverrideDisplacesOnlySameASKey(t *testing.T) {
	registry := newApplicationServers(DefaultRecoveryTimer, nil)

	first, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	first.as = registry
	if err := first.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("first contextless Override ASP Active: %v", err)
	}
	if err := first.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("first contextless Override state update: %v", err)
	}

	third, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	third.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	third.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	third.as = registry
	if err := third.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("third contextless Override ASP Active in another Network Appearance: %v", err)
	}
	if err := third.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("third contextless Override state update: %v", err)
	}

	second, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	second.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	second.as = registry
	if err := second.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("second contextless Override ASP Active: %v", err)
	}

	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true}
	got := registry.get(key).activeASPs()
	if len(got) != 1 || got[0] != second {
		t.Fatalf("contextless Override active ASPs = %v, want only second ASP", got)
	}
	if first.activeForASKey(key) {
		t.Fatal("first ASP remained locally active after contextless Override displacement")
	}
	otherKey := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true}
	if got := registry.get(otherKey).activeASPs(); len(got) != 1 || got[0] != third {
		t.Fatalf("foreign contextless Override active ASPs = %v, want only third ASP", got)
	}
	if !third.activeForASKey(otherKey) {
		t.Fatal("foreign ASP was locally inactivated by contextless Override displacement")
	}
}
