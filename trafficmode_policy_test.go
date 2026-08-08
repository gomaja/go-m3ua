package m3ua

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestConnFreezesTrafficModePolicyAtConstruction(t *testing.T) {
	config := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222,
		0x11111111,
		1,
		params.TrafficModeLoadshare,
		0,
		0,
		[]uint32{1, 2},
		params.ServiceIndSCCP,
		0,
		0,
		1,
	)
	configuredModes := map[uint32]uint32{
		1: params.TrafficModeOverride,
		2: params.TrafficModeBroadcast,
	}
	config.TrafficModes = configuredModes
	connection := newConn(modeClient, config)

	configuredModes[1] = params.TrafficModeBroadcast
	configuredModes[2] = params.TrafficModeOverride
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeBroadcast)
	config.TrafficModes = map[uint32]uint32{
		1: params.TrafficModeBroadcast,
		2: params.TrafficModeOverride,
	}

	requests, err := connection.aspActiveRequests(params.NewRoutingContext(1, 2))
	if err != nil {
		t.Fatalf("aspActiveRequests: %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("ASP Active requests = %d, want one per frozen traffic mode", len(requests))
	}
	want := map[uint32]uint32{
		1: params.TrafficModeOverride,
		2: params.TrafficModeBroadcast,
	}
	for _, request := range requests {
		contexts := request.routingContext.RoutingContexts()
		if len(contexts) != 1 {
			t.Fatalf("ASP Active request contexts = %v, want one context", contexts)
		}
		if request.trafficMode == nil {
			t.Fatalf("ASP Active request for RC %d omitted Traffic Mode Type", contexts[0])
		}
		if got := request.trafficMode.TrafficModeType(); got != want[contexts[0]] {
			t.Fatalf("ASP Active request mode for RC %d = %d, want frozen mode %d",
				contexts[0], got, want[contexts[0]])
		}
	}
}

func TestConnFreezesTrafficModePolicyForActiveAckValidation(t *testing.T) {
	config := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222,
		0x11111111,
		1,
		params.TrafficModeLoadshare,
		0,
		0,
		[]uint32{1},
		params.ServiceIndSCCP,
		0,
		0,
		1,
	)
	configuredDefault := config.TrafficModeType
	connection := newConn(modeClient, config)

	configuredDefault.Data[3] = byte(params.TrafficModeBroadcast)
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeBroadcast)
	config.TrafficModes = map[uint32]uint32{1: params.TrafficModeBroadcast}

	err := connection.validateAspActiveAckTrafficMode(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1),
		nil,
	))
	if err != nil {
		t.Fatalf("Ack matching the construction-time policy was rejected: %v", err)
	}
}

func TestApplicationServerRegistryFreezesTrafficModePolicyAtConstruction(t *testing.T) {
	config := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222,
		0x11111111,
		1,
		params.TrafficModeLoadshare,
		0,
		0,
		[]uint32{1},
		params.ServiceIndSCCP,
		0,
		0,
		1,
	)
	configuredModes := map[uint32]uint32{1: params.TrafficModeOverride}
	config.TrafficModes = configuredModes
	registry := newApplicationServers(time.Hour, config)

	configuredModes[1] = params.TrafficModeBroadcast
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeBroadcast)
	config.TrafficModes = map[uint32]uint32{1: params.TrafficModeBroadcast}

	agreed, err := registry.agreeTrafficMode([]uint32{1}, nil)
	if err != nil {
		t.Fatalf("agreeTrafficMode rejected the construction-time policy: %v", err)
	}
	if agreed == nil || agreed.TrafficModeType() != params.TrafficModeOverride {
		t.Fatalf("agreed Traffic Mode = %v, want frozen Override", agreed)
	}
}

func TestListenerTrafficModePolicyIsInheritedByRegistryAndAcceptedConnections(t *testing.T) {
	config := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222,
		0x11111111,
		1,
		params.TrafficModeLoadshare,
		0,
		0,
		[]uint32{1},
		params.ServiceIndSCCP,
		0,
		0,
		1,
	)
	configuredModes := map[uint32]uint32{1: params.TrafficModeOverride}
	config.TrafficModes = configuredModes
	listener := newListener(NewListenerConfig(config))

	configuredModes[1] = params.TrafficModeBroadcast
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeBroadcast)
	config.TrafficModes = map[uint32]uint32{1: params.TrafficModeBroadcast}

	registry, _, _ := listener.registry()
	agreed, err := registry.agreeTrafficMode([]uint32{1}, nil)
	if err != nil {
		t.Fatalf("listener registry agreement: %v", err)
	}
	if agreed == nil || agreed.TrafficModeType() != params.TrafficModeOverride {
		t.Fatalf("listener registry mode = %v, want construction-time Override", agreed)
	}

	accepted := newConnWithTrafficModePolicy(
		modeServer, listener.Config, listener.trafficModePolicy(),
	)
	requests, err := accepted.aspActiveRequests(params.NewRoutingContext(1))
	if err != nil {
		t.Fatalf("accepted Conn ASP Active request: %v", err)
	}
	if len(requests) != 1 || requests[0].trafficMode == nil ||
		requests[0].trafficMode.TrafficModeType() != params.TrafficModeOverride {
		t.Fatalf("accepted Conn inherited traffic mode %v, want construction-time Override", requests)
	}
}

func TestPerRoutingContextTrafficModePrecedesConfiguredDefault(t *testing.T) {
	connection, sent := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)
	connection.cfg.TrafficModes = map[uint32]uint32{
		1: params.TrafficModeOverride,
	}
	registry := newApplicationServers(time.Hour, connection.cfg)
	connection.as = registry
	registry.aspStateChanged(connection, StateAspInactive)

	for _, request := range []struct {
		routingContext uint32
		mode           uint32
	}{
		{routingContext: 1, mode: params.TrafficModeOverride},
		{routingContext: 2, mode: params.TrafficModeLoadshare},
	} {
		before := len(*sent)
		err := connection.handleAspActive(messages.NewAspActive(
			params.NewTrafficModeType(request.mode),
			params.NewRoutingContext(request.routingContext),
			nil,
		))
		if err != nil {
			t.Fatalf("RC %d mode %d: %v", request.routingContext, request.mode, err)
		}
		ack := lastAspActiveAck(t, (*sent)[before:])
		if ack.TrafficModeType == nil || ack.TrafficModeType.TrafficModeType() != request.mode {
			t.Fatalf("RC %d Ack mode = %v, want %d",
				request.routingContext, ack.TrafficModeType, request.mode)
		}
		if got := registry.get(request.routingContext).TrafficMode(); got != request.mode {
			t.Fatalf("RC %d agreed mode = %d, want %d",
				request.routingContext, got, request.mode)
		}
	}
}

func TestTrafficModeAgreementRejectsMixedScopeAtomically(t *testing.T) {
	connection, sent := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)
	connection.cfg.TrafficModes = map[uint32]uint32{
		1: params.TrafficModeOverride,
	}
	registry := newApplicationServers(time.Hour, connection.cfg)
	_, agreementErr := registry.agreeTrafficMode(
		[]uint32{1, 2}, params.NewTrafficModeType(params.TrafficModeOverride),
	)
	if !errors.Is(agreementErr, ErrUnsupportedTrafficMode) {
		t.Fatalf("direct mixed-scope agreement error = %v, want %v",
			agreementErr, ErrUnsupportedTrafficMode)
	}
	for _, routingContext := range []uint32{1, 2} {
		if got := registry.get(routingContext).TrafficMode(); got != 0 {
			t.Fatalf("direct rejected agreement committed mode %d for RC %d",
				got, routingContext)
		}
	}
	connection.as = registry
	registry.aspStateChanged(connection, StateAspInactive)

	before := len(*sent)
	err := connection.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1, 2),
		nil,
	))
	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("mixed-scope agreement error = %v, want %v", err, ErrUnsupportedTrafficMode)
	}
	for _, signal := range (*sent)[before:] {
		if _, ok := signal.(*messages.AspActiveAck); ok {
			t.Fatal("partially valid mixed-scope Traffic Mode was acknowledged")
		}
	}
	for _, routingContext := range []uint32{1, 2} {
		if got := registry.get(routingContext).TrafficMode(); got != 0 {
			t.Fatalf("rejected agreement committed mode %d for RC %d", got, routingContext)
		}
	}
}

func TestTrafficModePolicyIgnoresConcurrentConfigMutation(t *testing.T) {
	config := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222,
		0x11111111,
		1,
		params.TrafficModeLoadshare,
		0,
		0,
		[]uint32{1},
		params.ServiceIndSCCP,
		0,
		0,
		1,
	)
	config.TrafficModes = map[uint32]uint32{1: params.TrafficModeOverride}
	connection := newConn(modeClient, config)
	registry := newApplicationServers(time.Hour, config)
	ack := messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1),
		nil,
	)

	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for index := 0; index < 2_000; index++ {
			config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeBroadcast)
			config.TrafficModes = map[uint32]uint32{1: params.TrafficModeBroadcast}
			config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
			config.TrafficModes = map[uint32]uint32{1: params.TrafficModeOverride}
		}
	}()

	readErrors := make(chan error, 1)
	go func() {
		defer workers.Done()
		<-start
		for index := 0; index < 2_000; index++ {
			requests, err := connection.aspActiveRequests(params.NewRoutingContext(1))
			if err != nil {
				readErrors <- err
				return
			}
			if len(requests) != 1 || requests[0].trafficMode == nil ||
				requests[0].trafficMode.TrafficModeType() != params.TrafficModeOverride {
				readErrors <- errors.New("client request grouping observed a post-construction Config mutation")
				return
			}
			if err := connection.validateAspActiveAckTrafficMode(ack); err != nil {
				readErrors <- err
				return
			}
			if _, err := registry.agreeTrafficMode(
				[]uint32{1}, params.NewTrafficModeType(params.TrafficModeOverride),
			); err != nil {
				readErrors <- err
				return
			}
		}
	}()

	close(start)
	workers.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Fatalf("traffic-mode policy changed after construction: %v", err)
	}
}
