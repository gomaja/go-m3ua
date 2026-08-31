package m3ua

import (
	"sync"

	"github.com/gomaja/go-m3ua/messages/params"
)

// trafficModePolicy is the immutable traffic-handling policy resolved when a
// Association or Listener is constructed. AssociationConfig is intentionally
// public and may be
// reused by callers, so protocol goroutines must not retain live reads of its
// TrafficModeType Param or TrafficModes map.
type trafficModePolicy struct {
	defaultMode    uint32
	defaultModeSet bool
	modes          map[uint32]uint32
}

func newTrafficModePolicy(config *AssociationConfig) trafficModePolicy {
	policy := trafficModePolicy{}
	if config == nil {
		return policy
	}
	if config.TrafficModeType != nil {
		policy.defaultMode = config.TrafficModeType.TrafficModeType()
		policy.defaultModeSet = true
	}
	if len(config.TrafficModes) != 0 {
		policy.modes = make(map[uint32]uint32, len(config.TrafficModes))
		for routingContext, mode := range config.TrafficModes {
			policy.modes[routingContext] = mode
		}
	}
	return policy
}

func newIPSPTrafficModePolicy(config *IPSPTrafficConfig) trafficModePolicy {
	policy := trafficModePolicy{}
	if config == nil {
		return policy
	}
	if config.TrafficModeType != nil {
		policy.defaultMode = config.TrafficModeType.TrafficModeType()
		policy.defaultModeSet = true
	}
	if len(config.TrafficModes) != 0 {
		policy.modes = make(map[uint32]uint32, len(config.TrafficModes))
		for routingContext, mode := range config.TrafficModes {
			policy.modes[routingContext] = mode
		}
	}
	return policy
}

func (p trafficModePolicy) configured(routingContext uint32) (uint32, bool) {
	if mode, ok := p.modes[routingContext]; ok {
		return mode, true
	}
	return p.defaultMode, p.defaultModeSet
}

func (p trafficModePolicy) configuredForASKey(key ASKey) (uint32, bool) {
	if !key.RoutingContextSet {
		return p.defaultMode, p.defaultModeSet
	}
	return p.configured(key.RoutingContext)
}

func (p trafficModePolicy) defaultParam() *params.Param {
	if !p.defaultModeSet {
		return nil
	}
	return params.NewTrafficModeType(p.defaultMode)
}

// trafficModeSnapshot supplies a once-only fallback for package tests that
// build Association or Listener values directly. Production constructors freeze the
// snapshot before publishing either value.
type trafficModeSnapshot struct {
	once   sync.Once
	policy trafficModePolicy
}

func (s *trafficModeSnapshot) freeze(policy trafficModePolicy) {
	s.once.Do(func() {
		s.policy = policy
	})
}

func (s *trafficModeSnapshot) get(config *AssociationConfig) trafficModePolicy {
	s.once.Do(func() {
		s.policy = newTrafficModePolicy(config)
	})
	return s.policy
}
