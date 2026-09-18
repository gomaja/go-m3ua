package m3ua

import (
	"sync"

	"github.com/gomaja/go-m3ua/messages/params"
)

// trafficModePolicy is the immutable traffic-handling policy resolved when an
// Association or Listener is constructed. AssociationConfig is intentionally
// public and may be reused by callers, so protocol goroutines must not retain
// live reads of its Application Server inventory.
//
// contextlessMode is the mode agreed for the contextless Application Server of
// RFC 4666 Section 3.6.1. It is also what a Routing Context assigned later by
// Section 4.4.1 registration inherits, because an inventory that declares the
// contextless Application Server declares no Routing-Context-scoped one to
// disagree with it.
type trafficModePolicy struct {
	contextlessMode    uint32
	contextlessModeSet bool
	modes              map[uint32]uint32
}

func newTrafficModePolicy(config *AssociationConfig) trafficModePolicy {
	if config == nil {
		return trafficModePolicy{}
	}
	return newApplicationServerTrafficModePolicy(config.ApplicationServers)
}

func newIPSPTrafficModePolicy(config *IPSPTrafficConfig) trafficModePolicy {
	if config == nil {
		return trafficModePolicy{}
	}
	return newApplicationServerTrafficModePolicy(config.ApplicationServers)
}

func newApplicationServerTrafficModePolicy(servers []ASConfig) trafficModePolicy {
	policy := trafficModePolicy{}
	for _, server := range servers {
		if server.TrafficMode == 0 {
			continue
		}
		if !server.ASKey.RoutingContextSet {
			policy.contextlessMode = server.TrafficMode
			policy.contextlessModeSet = true
			continue
		}
		if policy.modes == nil {
			policy.modes = make(map[uint32]uint32, len(servers))
		}
		policy.modes[server.ASKey.RoutingContext] = server.TrafficMode
	}
	return policy
}

func (p trafficModePolicy) configured(routingContext uint32) (uint32, bool) {
	if mode, ok := p.modes[routingContext]; ok {
		return mode, true
	}
	return p.contextlessMode, p.contextlessModeSet
}

func (p trafficModePolicy) configuredForASKey(key ASKey) (uint32, bool) {
	if !key.RoutingContextSet {
		return p.contextlessMode, p.contextlessModeSet
	}
	return p.configured(key.RoutingContext)
}

// contextlessParam is the Traffic Mode Type parameter for the contextless
// Application Server, or nil when none was agreed for it.
func (p trafficModePolicy) contextlessParam() *params.Param {
	if !p.contextlessModeSet {
		return nil
	}
	return params.NewTrafficModeType(p.contextlessMode)
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
