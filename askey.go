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

func normalizeASKey(scope any) (ASKey, bool) {
	switch value := scope.(type) {
	case ASKey:
		return value, true
	case uint32:
		return routingContextASKey(value), true
	case int:
		if value < 0 {
			return ASKey{}, false
		}
		return routingContextASKey(uint32(value)), true
	default:
		return ASKey{}, false
	}
}

func asKeyForConfigRoutingContext(config *Config, routingContext uint32) ASKey {
	key := routingContextASKey(routingContext)
	if config != nil {
		key.NetworkAppearance, key.NetworkAppearanceSet = appearanceOf(config.NetworkAppearance)
	}
	return key
}

func contextlessASKeyForConfig(config *Config) ASKey {
	var key ASKey
	if config != nil {
		key.NetworkAppearance, key.NetworkAppearanceSet = appearanceOf(config.NetworkAppearance)
	}
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
