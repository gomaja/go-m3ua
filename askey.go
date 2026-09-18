package m3ua

import "github.com/gomaja/go-m3ua/messages/params"

// ASKey identifies one Application Server traffic scope.
//
// RFC 4666 Section 3.3.1 defines Network Appearance as the SS7 network
// context for a message, and Section 3.6.1 makes it part of Routing Key
// identity. A Routing Context value is therefore not globally unique by
// itself: the same value can identify different Application Servers in
// different Network Appearances.
type ASKey struct {
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	RoutingContext       uint32
	RoutingContextSet    bool
}

func routingContextASKey(routingContext uint32) ASKey {
	return ASKey{RoutingContext: routingContext, RoutingContextSet: true}
}

const maxRoutingContextValue = ^uint32(0)

func routingContextFromInt(value int) (uint32, bool) {
	if value < 0 || uint64(value) > uint64(maxRoutingContextValue) {
		return 0, false
	}
	return uint32(value), true
}

func normalizeASKey(scope any) (ASKey, bool) {
	switch value := scope.(type) {
	case ASKey:
		return value, true
	case uint32:
		return routingContextASKey(value), true
	case int:
		routingContext, ok := routingContextFromInt(value)
		if !ok {
			return ASKey{}, false
		}
		return routingContextASKey(routingContext), true
	default:
		return ASKey{}, false
	}
}

// asKeyForConfigRoutingContext returns the Application Server one Association
// configuration gives a Routing Context: the declaring entry's own ASKey when
// the inventory declares it, and otherwise that context in the Network
// Appearance the inventory shares.
func asKeyForConfigRoutingContext(config *AssociationConfig, routingContext uint32) ASKey {
	if config == nil {
		return routingContextASKey(routingContext)
	}
	if key, declared := asConfigASKeyFor(config.ApplicationServers, routingContext); declared {
		return key
	}
	key := routingContextASKey(routingContext)
	key.NetworkAppearance, key.NetworkAppearanceSet = asConfigNetworkAppearance(config.ApplicationServers)
	return key
}

func contextlessASKeyForConfig(config *AssociationConfig) ASKey {
	if config == nil {
		return ASKey{}
	}
	if key, declared := asConfigContextlessASKey(config.ApplicationServers); declared {
		return key
	}
	var key ASKey
	key.NetworkAppearance, key.NetworkAppearanceSet = asConfigNetworkAppearance(config.ApplicationServers)
	return key
}

func routingContextParamForASKey(key ASKey) *params.Param {
	if !key.RoutingContextSet {
		return nil
	}
	return params.NewRoutingContext(key.RoutingContext)
}

func compareASKey(first, second ASKey) int {
	if first.NetworkAppearanceSet != second.NetworkAppearanceSet {
		if !first.NetworkAppearanceSet {
			return -1
		}
		return 1
	}
	if first.NetworkAppearance != second.NetworkAppearance {
		if first.NetworkAppearance < second.NetworkAppearance {
			return -1
		}
		return 1
	}
	if first.RoutingContextSet != second.RoutingContextSet {
		if !first.RoutingContextSet {
			return -1
		}
		return 1
	}
	if first.RoutingContext != second.RoutingContext {
		if first.RoutingContext < second.RoutingContext {
			return -1
		}
		return 1
	}
	return 0
}
